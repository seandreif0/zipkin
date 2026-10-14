/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.storage.redis;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import zipkin2.Call;
import zipkin2.Span;
import zipkin2.codec.SpanBytesEncoder;
import zipkin2.storage.SpanConsumer;

final class RedisSpanConsumer implements SpanConsumer {
  final RedisStorage storage;

  RedisSpanConsumer(RedisStorage storage) {
    this.storage = storage;
  }

  @Override public Call<Void> accept(List<Span> spans) {
    if (spans.isEmpty()) return Call.create(null);
    return storage.callFactory.create(new StoreSpans(storage, spans));
  }

  static final class StoreSpans implements java.util.function.Function<Jedis, Void> {
    final RedisStorage storage;
    final List<Span> spans;

    StoreSpans(RedisStorage storage, List<Span> spans) {
      this.storage = storage;
      this.spans = spans;
    }

    @Override public Void apply(Jedis jedis) {
      Set<String> touchedKeys = new LinkedHashSet<>();
      Pipeline pipeline = jedis.pipelined();
      for (Span span : spans) {
        String lowTraceId = RedisKeys.lowTraceId(span.traceId());
        long timestampMillis = span.timestampAsLong() / 1000L;
        String member = RedisKeys.indexMember(lowTraceId, timestampMillis);

        String traceKey = RedisKeys.trace(lowTraceId);
        pipeline.sadd(traceKey,
          new String(SpanBytesEncoder.JSON_V2.encode(span), StandardCharsets.UTF_8));
        touchedKeys.add(traceKey);

        pipeline.zadd(RedisKeys.TRACE_INDEX, (double) timestampMillis, member);
        touchedKeys.add(RedisKeys.TRACE_INDEX);

        if (!storage.searchEnabled) continue;

        String serviceName = span.localServiceName();
        if (serviceName != null) {
          String serviceIndex = RedisKeys.serviceIndex(serviceName);
          pipeline.zadd(serviceIndex, (double) timestampMillis, member);
          pipeline.sadd(RedisKeys.SERVICES, serviceName);
          touchedKeys.add(serviceIndex);
          touchedKeys.add(RedisKeys.SERVICES);

          String spanName = span.name();
          if (spanName != null) {
            String key = RedisKeys.spanNames(serviceName);
            pipeline.sadd(key, spanName);
            touchedKeys.add(key);
          }
          String remoteServiceName = span.remoteServiceName();
          if (remoteServiceName != null) {
            String key = RedisKeys.remoteServiceNames(serviceName);
            pipeline.sadd(key, remoteServiceName);
            touchedKeys.add(key);
          }
        }

        for (Map.Entry<String, String> tag : span.tags().entrySet()) {
          if (!storage.autocompleteKeys.contains(tag.getKey())) continue;
          String key = RedisKeys.autocomplete(tag.getKey());
          pipeline.sadd(key, tag.getValue());
          touchedKeys.add(key);
        }
      }

      if (storage.ttlSeconds > 0) {
        for (String key : touchedKeys) pipeline.expire(key, storage.ttlSeconds);
      }
      pipeline.sync();
      return null;
    }

    @Override public String toString() {
      return "StoreSpans{" + spans.size() + " spans}";
    }
  }

  @Override public String toString() {
    return "RedisSpanConsumer{" + storage + "}";
  }
}
