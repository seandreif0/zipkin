/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.storage.redis;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import zipkin2.Call;
import zipkin2.DependencyLink;
import zipkin2.Span;
import zipkin2.codec.SpanBytesDecoder;
import zipkin2.internal.DependencyLinker;
import zipkin2.storage.QueryRequest;
import zipkin2.storage.ServiceAndSpanNames;
import zipkin2.storage.SpanStore;
import zipkin2.storage.Traces;

final class RedisSpanStore implements SpanStore, Traces, ServiceAndSpanNames {
  /** How many index entries to read per round trip when searching for traces. */
  static final int INDEX_PAGE_SIZE = 500;

  final RedisStorage storage;
  final boolean strictTraceId, searchEnabled;

  RedisSpanStore(RedisStorage storage) {
    this.storage = storage;
    this.strictTraceId = storage.strictTraceId;
    this.searchEnabled = storage.searchEnabled;
  }

  @Override public Call<List<List<Span>>> getTraces(QueryRequest request) {
    if (!searchEnabled) return Call.emptyList();
    String indexKey = request.serviceName() != null
      ? RedisKeys.serviceIndex(request.serviceName())
      : RedisKeys.TRACE_INDEX;
    long endTs = request.endTs(), beginTs = Math.max(0L, endTs - request.lookback());
    return storage.callFactory.create(jedis -> {
      List<List<Span>> result = new ArrayList<>();
      Set<String> seen = new LinkedHashSet<>();
      int offset = 0;
      while (result.size() < request.limit()) {
        List<String> members =
          jedis.zrevrangeByScore(indexKey, (double) endTs, (double) beginTs, offset, INDEX_PAGE_SIZE);
        if (members.isEmpty()) break;
        offset += members.size();

        Set<String> lowTraceIds = new LinkedHashSet<>();
        for (String member : members) {
          String lowTraceId = RedisKeys.lowTraceIdFromIndexMember(member);
          if (seen.add(lowTraceId)) lowTraceIds.add(lowTraceId);
        }

        for (List<Span> next : spansByLowTraceId(jedis, lowTraceIds).values()) {
          if (result.size() >= request.limit()) break;
          if (!request.test(next)) continue;
          if (!strictTraceId) {
            result.add(next);
            continue;
          }
          // re-run the query as now spans are strictly grouped
          for (List<Span> strictTrace : strictByTraceId(next)) {
            if (request.test(strictTrace)) result.add(strictTrace);
          }
        }
        if (members.size() < INDEX_PAGE_SIZE) break;
      }
      return result;
    });
  }

  @Override public Call<List<Span>> getTrace(String traceId) {
    String normalized = Span.normalizeTraceId(traceId);
    String lowTraceId = RedisKeys.lowTraceId(normalized);
    return storage.callFactory.create(jedis -> {
      List<Span> spans = decode(jedis.smembers(RedisKeys.trace(lowTraceId)));
      if (!strictTraceId) return spans;
      List<Span> result = new ArrayList<>();
      for (Span span : spans) {
        if (span.traceId().equals(normalized)) result.add(span);
      }
      return result;
    });
  }

  @Override public Call<List<List<Span>>> getTraces(Iterable<String> traceIds) {
    Set<String> normalized = new LinkedHashSet<>();
    Set<String> lowTraceIds = new LinkedHashSet<>();
    for (String traceId : traceIds) {
      String next = Span.normalizeTraceId(traceId);
      normalized.add(next);
      lowTraceIds.add(RedisKeys.lowTraceId(next));
    }
    if (normalized.isEmpty()) return Call.emptyList();
    return storage.callFactory.create(jedis -> {
      List<List<Span>> result = new ArrayList<>();
      for (List<Span> sameLowTraceId : spansByLowTraceId(jedis, lowTraceIds).values()) {
        if (!strictTraceId) {
          result.add(sameLowTraceId);
          continue;
        }
        for (List<Span> trace : strictByTraceId(sameLowTraceId)) {
          if (normalized.contains(trace.get(0).traceId())) result.add(trace);
        }
      }
      return result;
    });
  }

  @Override public Call<List<String>> getServiceNames() {
    if (!searchEnabled) return Call.emptyList();
    return sortedMembers(RedisKeys.SERVICES);
  }

  @Override public Call<List<String>> getRemoteServiceNames(String serviceName) {
    if (serviceName.isEmpty() || !searchEnabled) return Call.emptyList();
    // service names are always lowercase!
    return sortedMembers(RedisKeys.remoteServiceNames(serviceName.toLowerCase(Locale.ROOT)));
  }

  @Override public Call<List<String>> getSpanNames(String serviceName) {
    if (serviceName.isEmpty() || !searchEnabled) return Call.emptyList();
    // service names are always lowercase!
    return sortedMembers(RedisKeys.spanNames(serviceName.toLowerCase(Locale.ROOT)));
  }

  @Override public Call<List<DependencyLink>> getDependencies(long endTs, long lookback) {
    if (endTs <= 0) throw new IllegalArgumentException("endTs <= 0");
    if (lookback <= 0) throw new IllegalArgumentException("lookback <= 0");
    long beginTs = Math.max(0L, endTs - lookback);
    return storage.callFactory.create(jedis -> {
      Set<String> lowTraceIds = new LinkedHashSet<>();
      for (String member : jedis.zrevrangeByScore(RedisKeys.TRACE_INDEX, (double) endTs, (double) beginTs)) {
        lowTraceIds.add(RedisKeys.lowTraceIdFromIndexMember(member));
      }
      if (lowTraceIds.isEmpty()) return Collections.emptyList();
      // We don't have a query parameter for strictTraceId when fetching dependency links, so we
      // ignore traceIdHigh. Otherwise, a single trace can appear as two, doubling callCount.
      DependencyLinker linker = new DependencyLinker();
      for (List<Span> trace : spansByLowTraceId(jedis, lowTraceIds).values()) {
        linker.putTrace(trace);
      }
      return linker.link();
    });
  }

  Call<List<String>> sortedMembers(String key) {
    return storage.callFactory.create(jedis -> {
      List<String> result = new ArrayList<>(jedis.smembers(key));
      Collections.sort(result);
      return result;
    });
  }

  /** Returns non-empty traces in iteration order of the input. Expired traces are skipped. */
  static Map<String, List<Span>> spansByLowTraceId(Jedis jedis, Collection<String> lowTraceIds) {
    Map<String, Response<Set<String>>> responses = new LinkedHashMap<>();
    Pipeline pipeline = jedis.pipelined();
    for (String lowTraceId : lowTraceIds) {
      responses.put(lowTraceId, pipeline.smembers(RedisKeys.trace(lowTraceId)));
    }
    pipeline.sync();

    Map<String, List<Span>> result = new LinkedHashMap<>();
    for (Map.Entry<String, Response<Set<String>>> entry : responses.entrySet()) {
      List<Span> spans = decode(entry.getValue().get());
      if (!spans.isEmpty()) result.put(entry.getKey(), spans);
    }
    return result;
  }

  static List<Span> decode(Set<String> encoded) {
    List<Span> result = new ArrayList<>(encoded.size());
    for (String json : encoded) {
      result.add(SpanBytesDecoder.JSON_V2.decodeOne(json.getBytes(StandardCharsets.UTF_8)));
    }
    return result;
  }

  static Collection<List<Span>> strictByTraceId(List<Span> next) {
    Map<String, List<Span>> groupedByTraceId = new LinkedHashMap<>();
    for (Span span : next) {
      groupedByTraceId.computeIfAbsent(span.traceId(), k -> new ArrayList<>()).add(span);
    }
    return groupedByTraceId.values();
  }

  @Override public String toString() {
    return "RedisSpanStore{" + storage + "}";
  }
}
