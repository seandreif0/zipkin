/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.storage.redis;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import redis.clients.jedis.AbstractPipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.resps.Tuple;
import zipkin2.Call;
import zipkin2.DependencyLink;
import zipkin2.Span;
import zipkin2.codec.SpanBytesDecoder;
import zipkin2.internal.DependencyLinker;
import zipkin2.storage.QueryRequest;
import zipkin2.storage.ServiceAndSpanNames;
import zipkin2.storage.SpanStore;
import zipkin2.storage.Traces;

import static java.nio.charset.StandardCharsets.UTF_8;
import static zipkin2.storage.redis.RedisAutocompleteTags.sorted;
import static zipkin2.storage.redis.RedisSpanConsumer.DAY_MICROS;
import static zipkin2.storage.redis.RedisSpanConsumer.lowTraceId;

final class RedisSpanStore implements SpanStore, Traces, ServiceAndSpanNames {
  static final int PAGE_SIZE = 500;

  final RedisStorage storage;
  final RedisKeys keys;
  final boolean strictTraceId, searchEnabled;
  final List<String> autocompleteKeys;

  RedisSpanStore(RedisStorage storage) {
    this.storage = storage;
    this.keys = storage.keys;
    this.strictTraceId = storage.strictTraceId;
    this.searchEnabled = storage.searchEnabled;
    this.autocompleteKeys = storage.autocompleteKeys;
  }

  @Override public Call<List<List<Span>>> getTraces(QueryRequest request) {
    if (!searchEnabled) return Call.emptyList();
    return new RedisCall<>(storage, "getTraces", jedis -> query(jedis, request));
  }

  @Override public Call<List<Span>> getTrace(String traceId) {
    String normalized = Span.normalizeTraceId(traceId);
    return new RedisCall<>(storage, "getTrace", jedis -> {
      List<Span> spans = fetch(jedis, List.of(lowTraceId(normalized))).get(0);
      if (!strictTraceId || spans.isEmpty()) return spans;
      List<Span> result = new ArrayList<>(spans.size());
      for (Span span : spans) {
        if (span.traceId().equals(normalized)) result.add(span);
      }
      return result;
    });
  }

  @Override public Call<List<List<Span>>> getTraces(Iterable<String> traceIds) {
    Set<String> normalized = new LinkedHashSet<>(), lowTraceIds = new LinkedHashSet<>();
    for (String traceId : traceIds) {
      String next = Span.normalizeTraceId(traceId);
      normalized.add(next);
      lowTraceIds.add(lowTraceId(next));
    }
    if (normalized.isEmpty()) return Call.emptyList();
    List<String> toFetch = List.copyOf(lowTraceIds);
    return new RedisCall<>(storage, "getTraces", jedis -> {
      List<List<Span>> result = new ArrayList<>();
      for (List<Span> sameTraceId : fetch(jedis, toFetch)) {
        if (sameTraceId.isEmpty()) continue;
        if (!strictTraceId) {
          result.add(sameTraceId);
          continue;
        }
        for (List<Span> trace : strictByTraceId(sameTraceId)) {
          if (normalized.contains(trace.get(0).traceId())) result.add(trace);
        }
      }
      return result;
    });
  }

  @Override public Call<List<String>> getServiceNames() {
    if (!searchEnabled) return Call.emptyList();
    String key = keys.serviceNames();
    return new RedisCall<>(storage, "getServiceNames", jedis -> sorted(jedis.smembers(key)));
  }

  @Override public Call<List<String>> getRemoteServiceNames(String serviceName) {
    if (serviceName == null || serviceName.isEmpty() || !searchEnabled) return Call.emptyList();
    String key = keys.remoteServiceNames(serviceName.toLowerCase(java.util.Locale.ROOT));
    return new RedisCall<>(storage, "getRemoteServiceNames", jedis -> sorted(jedis.smembers(key)));
  }

  @Override public Call<List<String>> getSpanNames(String serviceName) {
    if (serviceName == null || serviceName.isEmpty() || !searchEnabled) return Call.emptyList();
    String key = keys.spanNames(serviceName.toLowerCase(java.util.Locale.ROOT));
    return new RedisCall<>(storage, "getSpanNames", jedis -> sorted(jedis.smembers(key)));
  }

  @Override public Call<List<DependencyLink>> getDependencies(long endTs, long lookback) {
    if (endTs <= 0) throw new IllegalArgumentException("endTs <= 0");
    if (lookback <= 0) throw new IllegalArgumentException("lookback <= 0");
    return new RedisCall<>(storage, "getDependencies", jedis -> {
      List<String> members = jedis.zrangeByScore(keys.timestampIndex(),
        minTimestamp(endTs, lookback), maxTimestamp(endTs));
      Set<String> lowTraceIds = new LinkedHashSet<>();
      for (String member : members) lowTraceIds.add(lowTraceIdOf(member));
      if (lowTraceIds.isEmpty()) return Collections.emptyList();

      // Like other backends, ignore the high bits so that one trace isn't counted twice
      DependencyLinker linker = new DependencyLinker();
      List<String> all = List.copyOf(lowTraceIds);
      for (int i = 0; i < all.size(); i += PAGE_SIZE) {
        for (List<Span> trace : fetch(jedis, all.subList(i, Math.min(all.size(), i + PAGE_SIZE)))) {
          if (!trace.isEmpty()) linker.putTrace(trace);
        }
      }
      return linker.link();
    });
  }

  List<List<Span>> query(UnifiedJedis jedis, QueryRequest request) {
    List<List<Span>> result = new ArrayList<>();
    Set<String> seen = new LinkedHashSet<>();
    double min = minTimestamp(request.endTs(), request.lookback());
    double max = maxTimestamp(request.endTs());

    if (request.minDuration() != null) {
      for (List<String> page : paginate(durationCandidates(jedis, request, min, max))) {
        if (addMatches(jedis, request, page, seen, result)) break;
      }
      return result;
    }

    String index = timestampIndex(request);
    for (int offset = 0; ; offset += PAGE_SIZE) {
      List<String> members = jedis.zrevrangeByScore(index, max, min, offset, PAGE_SIZE);
      if (addMatches(jedis, request, members, seen, result)) break;
      if (members.size() < PAGE_SIZE) break;
    }
    return result;
  }

  /** Picks the most selective timestamp index. {@link QueryRequest#test} applies the rest. */
  String timestampIndex(QueryRequest request) {
    String serviceName = request.serviceName();
    if (serviceName != null) {
      if (request.spanName() != null) return keys.spanIndex(serviceName, request.spanName());
      if (request.remoteServiceName() != null) {
        return keys.remoteServiceIndex(serviceName, request.remoteServiceName());
      }
    }
    for (Map.Entry<String, String> entry : request.annotationQuery().entrySet()) {
      if (!entry.getValue().isEmpty() && autocompleteKeys.contains(entry.getKey())) {
        return keys.tagIndex(entry.getKey(), entry.getValue());
      }
    }
    if (serviceName != null) return keys.serviceIndex(serviceName);
    return keys.timestampIndex();
  }

  /** Returns members in the duration range, newest first. */
  List<String> durationCandidates(UnifiedJedis jedis, QueryRequest request, double min,
    double max) {
    // Avoid scanning empty day buckets before the oldest indexed span
    List<Tuple> oldest = jedis.zrangeWithScores(keys.timestampIndex(), 0, 0);
    if (oldest.isEmpty()) return Collections.emptyList();
    min = Math.max(min, oldest.get(0).getScore());
    if (min > max) return Collections.emptyList();

    double minDuration = request.minDuration();
    double maxDuration =
      request.maxDuration() != null ? request.maxDuration() : Double.POSITIVE_INFINITY;
    List<Response<List<String>>> responses = new ArrayList<>();
    try (AbstractPipeline pipeline = jedis.pipelined()) {
      for (long day = (long) max / DAY_MICROS; day >= (long) min / DAY_MICROS; day--) {
        String key = keys.durationIndex(request.serviceName(), day);
        responses.add(pipeline.zrangeByScore(key, minDuration, maxDuration));
      }
      pipeline.sync();
    }

    List<String> result = new ArrayList<>();
    for (Response<List<String>> response : responses) {
      for (String member : response.get()) {
        long timestamp = timestampOf(member);
        if (timestamp >= min && timestamp <= max) result.add(member);
      }
    }
    result.sort(Comparator.comparingLong(RedisSpanStore::timestampOf).reversed());
    return result;
  }

  /** Returns true when the result is at its limit. */
  boolean addMatches(UnifiedJedis jedis, QueryRequest request, List<String> members,
    Set<String> seen, List<List<Span>> result) {
    List<String> lowTraceIds = new ArrayList<>();
    for (String member : members) {
      String lowTraceId = lowTraceIdOf(member);
      if (seen.add(lowTraceId)) lowTraceIds.add(lowTraceId);
    }
    if (lowTraceIds.isEmpty()) return result.size() >= request.limit();

    for (List<Span> next : fetch(jedis, lowTraceIds)) {
      if (result.size() >= request.limit()) return true;
      if (next.isEmpty() || !request.test(next)) continue;
      if (!strictTraceId) {
        result.add(next);
        continue;
      }
      // re-run the query as now spans are strictly grouped
      for (List<Span> strictTrace : strictByTraceId(next)) {
        if (request.test(strictTrace)) result.add(strictTrace);
      }
    }
    return result.size() >= request.limit();
  }

  /** Returns spans for each lower 64-bit trace ID, in the same order. */
  List<List<Span>> fetch(UnifiedJedis jedis, List<String> lowTraceIds) {
    List<Response<Set<String>>> responses = new ArrayList<>(lowTraceIds.size());
    try (AbstractPipeline pipeline = jedis.pipelined()) {
      for (String lowTraceId : lowTraceIds) {
        responses.add(pipeline.smembers(keys.trace(lowTraceId)));
      }
      pipeline.sync();
    }
    List<List<Span>> result = new ArrayList<>(responses.size());
    for (Response<Set<String>> response : responses) {
      Set<String> encoded = response.get();
      List<Span> spans = new ArrayList<>(encoded.size());
      for (String json : encoded) {
        spans.add(SpanBytesDecoder.JSON_V2.decodeOne(json.getBytes(UTF_8)));
      }
      result.add(spans);
    }
    return result;
  }

  static Collection<List<Span>> strictByTraceId(List<Span> spans) {
    Map<String, List<Span>> groupedByTraceId = new LinkedHashMap<>();
    for (Span span : spans) {
      groupedByTraceId.computeIfAbsent(span.traceId(), k -> new ArrayList<>()).add(span);
    }
    return groupedByTraceId.values();
  }

  static List<List<String>> paginate(List<String> members) {
    List<List<String>> pages = new ArrayList<>();
    for (int i = 0; i < members.size(); i += PAGE_SIZE) {
      pages.add(members.subList(i, Math.min(members.size(), i + PAGE_SIZE)));
    }
    return pages;
  }

  static double minTimestamp(long endTs, long lookback) {
    return Math.max(0, endTs - lookback) * 1000d;
  }

  static double maxTimestamp(long endTs) {
    return endTs * 1000d + 999;
  }

  static String lowTraceIdOf(String member) {
    return member.substring(0, member.indexOf(':'));
  }

  static long timestampOf(String member) {
    return Long.parseLong(member.substring(member.indexOf(':') + 1));
  }
}
