/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.storage.redis;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;
import zipkin2.Call;
import zipkin2.CheckResult;
import zipkin2.storage.AutocompleteTags;
import zipkin2.storage.ServiceAndSpanNames;
import zipkin2.storage.SpanConsumer;
import zipkin2.storage.SpanStore;
import zipkin2.storage.StorageComponent;
import zipkin2.storage.Traces;

/**
 * Stores spans in Redis, keyed by the lower 64-bits of their trace ID, with secondary indexes in
 * sorted sets (scored by span timestamp in epoch milliseconds) and sets.
 *
 * <p>The {@link JedisPool} is owned by the caller and not closed by {@link #close()}.
 */
public final class RedisStorage extends StorageComponent {
  /** 7 days */
  static final long DEFAULT_TTL_SECONDS = 7 * 24 * 60 * 60;

  public static Builder newBuilder() {
    return new Builder();
  }

  public static final class Builder extends StorageComponent.Builder {
    boolean strictTraceId = true, searchEnabled = true;
    List<String> autocompleteKeys = new ArrayList<>();
    long ttlSeconds = DEFAULT_TTL_SECONDS;
    JedisPool jedisPool;
    Executor executor;

    @Override public Builder strictTraceId(boolean strictTraceId) {
      this.strictTraceId = strictTraceId;
      return this;
    }

    @Override public Builder searchEnabled(boolean searchEnabled) {
      this.searchEnabled = searchEnabled;
      return this;
    }

    @Override public Builder autocompleteKeys(List<String> keys) {
      if (keys == null) throw new NullPointerException("keys == null");
      this.autocompleteKeys = keys;
      return this;
    }

    /** Pool of connections to Redis. Not closed by {@link RedisStorage#close()}. */
    public Builder jedisPool(JedisPool jedisPool) {
      if (jedisPool == null) throw new NullPointerException("jedisPool == null");
      this.jedisPool = jedisPool;
      return this;
    }

    /** Executor used when calls are enqueued (asynchronous). */
    public Builder executor(Executor executor) {
      if (executor == null) throw new NullPointerException("executor == null");
      this.executor = executor;
      return this;
    }

    /** Seconds to retain trace data and indexes after their last write. Zero disables expiry. */
    public Builder ttlSeconds(long ttlSeconds) {
      if (ttlSeconds < 0) throw new IllegalArgumentException("ttlSeconds < 0");
      this.ttlSeconds = ttlSeconds;
      return this;
    }

    @Override public RedisStorage build() {
      return new RedisStorage(this);
    }

    Builder() {
    }
  }

  final JedisPool jedisPool;
  final RedisCall.Factory callFactory;
  final boolean strictTraceId, searchEnabled;
  final List<String> autocompleteKeys;
  final long ttlSeconds;

  RedisStorage(Builder builder) {
    jedisPool = builder.jedisPool;
    if (jedisPool == null) throw new NullPointerException("jedisPool == null");
    Executor executor = builder.executor;
    if (executor == null) throw new NullPointerException("executor == null");
    callFactory = new RedisCall.Factory(jedisPool, executor);
    strictTraceId = builder.strictTraceId;
    searchEnabled = builder.searchEnabled;
    autocompleteKeys = List.copyOf(builder.autocompleteKeys);
    ttlSeconds = builder.ttlSeconds;
  }

  /** Returns the connection pool in use by this storage component. */
  public JedisPool jedisPool() {
    return jedisPool;
  }

  @Override public SpanStore spanStore() {
    return new RedisSpanStore(this);
  }

  @Override public Traces traces() {
    return (Traces) spanStore();
  }

  @Override public ServiceAndSpanNames serviceAndSpanNames() {
    return (ServiceAndSpanNames) spanStore();
  }

  @Override public AutocompleteTags autocompleteTags() {
    return new RedisAutocompleteTags(this);
  }

  @Override public SpanConsumer spanConsumer() {
    return new RedisSpanConsumer(this);
  }

  @Override public CheckResult check() {
    try (Jedis jedis = jedisPool.getResource()) {
      jedis.ping();
    } catch (Throwable e) {
      Call.propagateIfFatal(e);
      return CheckResult.failed(e);
    }
    return CheckResult.OK;
  }

  @Override public String toString() {
    return "RedisStorage{ttlSeconds=" + ttlSeconds + "}";
  }

  @Override public void close() {
    // didn't open the JedisPool or executor
  }

  /** Visible for testing: deletes all keys written by this component. */
  void clear() {
    try (Jedis jedis = jedisPool.getResource()) {
      ScanParams params = new ScanParams().match(RedisKeys.PREFIX + "*").count(1000);
      String cursor = ScanParams.SCAN_POINTER_START;
      do {
        ScanResult<String> page = jedis.scan(cursor, params);
        if (!page.getResult().isEmpty()) jedis.del(page.getResult().toArray(new String[0]));
        cursor = page.getCursor();
      } while (!ScanParams.SCAN_POINTER_START.equals(cursor));
    }
  }
}
