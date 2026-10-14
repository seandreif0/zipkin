/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.storage.redis;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import redis.clients.jedis.AbstractPipeline;
import redis.clients.jedis.UnifiedJedis;
import zipkin2.Call;
import zipkin2.Span;
import zipkin2.codec.SpanBytesEncoder;
import zipkin2.internal.DelayLimiter;
import zipkin2.storage.SpanConsumer;

import static java.nio.charset.StandardCharsets.UTF_8;

final class RedisSpanConsumer implements SpanConsumer {
  static final long DAY_MICROS = TimeUnit.DAYS.toMicros(1);

  final RedisStorage storage;
  final RedisKeys keys;
  final boolean searchEnabled;
  final List<String> autocompleteKeys;
  final int ttl;
  /** Suppresses repeated writes of the same name or autocomplete value. Context is key + value */
  final DelayLimiter<Map.Entry<String, String>> delayLimiter;

  RedisSpanConsumer(RedisStorage storage) {
    this.storage = storage;
    this.keys = storage.keys;
    this.searchEnabled = storage.searchEnabled;
    this.autocompleteKeys = storage.autocompleteKeys;
    this.ttl = storage.ttl;
    this.delayLimiter = DelayLimiter.newBuilder()
      // Re-add names before their Redis keys expire
      .ttl(ttl > 0 ? Math.min(storage.autocompleteTtl, ttl * 1000L) : storage.autocompleteTtl,
        TimeUnit.MILLISECONDS)
      .cardinality(storage.autocompleteCardinality).build();
  }

  @Override public Call<Void> accept(List<Span> spans) {
    if (spans.isEmpty()) return Call.create(null);
    List<Span> copy = List.copyOf(spans);
    return new RedisCall<>(storage, "accept", jedis -> {
      store(jedis, copy);
      return null;
    });
  }

  void store(UnifiedJedis jedis, List<Span> spans) {
    Set<String> expiring = new LinkedHashSet<>(), trimmed = new LinkedHashSet<>();
    Map<String, Long> durationIndexes = new LinkedHashMap<>();
    Map<String, Set<String>> names = new LinkedHashMap<>();
    List<Map.Entry<String, String>> limited = new ArrayList<>();
    try (AbstractPipeline pipeline = jedis.pipelined()) {
      for (Span span : spans) {
        String lowTraceId = lowTraceId(span.traceId());
        String traceKey = keys.trace(lowTraceId);
        pipeline.sadd(traceKey, new String(SpanBytesEncoder.JSON_V2.encode(span), UTF_8));
        expiring.add(traceKey);

        long timestamp = span.timestampAsLong();
        if (timestamp == 0L) continue; // can't index without a timestamp
        String member = lowTraceId + ":" + timestamp;

        // Always indexed as getDependencies relies on it, even when search is disabled
        zadd(pipeline, trimmed, keys.timestampIndex(), timestamp, member);
        if (!searchEnabled) continue;

        String serviceName = span.localServiceName();
        if (serviceName != null) {
          zadd(pipeline, trimmed, keys.serviceIndex(serviceName), timestamp, member);
          addName(names, keys.serviceNames(), serviceName);
          String spanName = span.name();
          if (spanName != null) {
            zadd(pipeline, trimmed, keys.spanIndex(serviceName, spanName), timestamp, member);
            addName(names, keys.spanNames(serviceName), spanName);
          }
          String remoteServiceName = span.remoteServiceName();
          if (remoteServiceName != null) {
            zadd(pipeline, trimmed, keys.remoteServiceIndex(serviceName, remoteServiceName),
              timestamp, member);
            addName(names, keys.remoteServiceNames(serviceName), remoteServiceName);
          }
        }

        long duration = span.durationAsLong();
        if (duration != 0L) {
          long epochDay = timestamp / DAY_MICROS;
          String key = keys.durationIndex(null, epochDay);
          pipeline.zadd(key, duration, member);
          durationIndexes.put(key, epochDay);
          if (serviceName != null) {
            key = keys.durationIndex(serviceName, epochDay);
            pipeline.zadd(key, duration, member);
            durationIndexes.put(key, epochDay);
          }
        }

        for (Map.Entry<String, String> tag : span.tags().entrySet()) {
          if (!autocompleteKeys.contains(tag.getKey())) continue;
          zadd(pipeline, trimmed, keys.tagIndex(tag.getKey(), tag.getValue()), timestamp, member);
          addName(names, keys.autocompleteValues(tag.getKey()), tag.getValue());
        }
      }

      for (Map.Entry<String, Set<String>> entry : names.entrySet()) {
        List<String> toAdd = new ArrayList<>();
        for (String value : entry.getValue()) {
          Map.Entry<String, String> context = Map.entry(entry.getKey(), value);
          if (!delayLimiter.shouldInvoke(context)) continue;
          limited.add(context);
          toAdd.add(value);
        }
        if (toAdd.isEmpty()) continue;
        pipeline.sadd(entry.getKey(), toAdd.toArray(new String[0]));
        expiring.add(entry.getKey());
      }

      if (ttl > 0) {
        double expiredBefore = (System.currentTimeMillis() - ttl * 1000L) * 1000d;
        for (String key : trimmed) {
          pipeline.zremrangeByScore(key, Double.NEGATIVE_INFINITY, expiredBefore);
          pipeline.expire(key, ttl);
        }
        for (String key : expiring) pipeline.expire(key, ttl);
        // A bucket holds a day of data, so expire it with its last entry, even when backfilled
        for (Map.Entry<String, Long> entry : durationIndexes.entrySet()) {
          long dayEnd = TimeUnit.DAYS.toSeconds(entry.getValue() + 1);
          pipeline.expireAt(entry.getKey(), dayEnd + ttl);
        }
      }
      pipeline.sync();
    } catch (RuntimeException e) {
      for (Map.Entry<String, String> context : limited) delayLimiter.invalidate(context);
      throw e;
    }
  }

  static void zadd(AbstractPipeline pipeline, Set<String> touched, String key, double score,
    String member) {
    pipeline.zadd(key, score, member);
    touched.add(key);
  }

  static void addName(Map<String, Set<String>> names, String key, String value) {
    names.computeIfAbsent(key, k -> new LinkedHashSet<>()).add(value);
  }

  static String lowTraceId(String traceId) {
    return traceId.length() == 32 ? traceId.substring(16) : traceId;
  }

  void clear() {
    delayLimiter.clear();
  }
}
