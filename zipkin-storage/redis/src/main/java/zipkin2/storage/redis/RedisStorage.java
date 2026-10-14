/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.storage.redis;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import redis.clients.jedis.ConnectionPoolConfig;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.exceptions.JedisBusyException;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;
import zipkin2.Call;
import zipkin2.CheckResult;
import zipkin2.internal.ClosedComponentException;
import zipkin2.internal.Nullable;
import zipkin2.storage.AutocompleteTags;
import zipkin2.storage.ServiceAndSpanNames;
import zipkin2.storage.SpanConsumer;
import zipkin2.storage.SpanStore;
import zipkin2.storage.StorageComponent;
import zipkin2.storage.Traces;

/**
 * Stores spans as JSON in Redis, with sorted-set indexes to support search.
 *
 * <p>Spans are grouped by the lower 64 bits of their trace ID in a set per trace. Search indexes
 * are sorted sets whose members are {@code <lowTraceId>:<timestampMicros>}, scored by timestamp
 * (or duration for duration queries). When {@link Builder#ttl(int)} is positive, all keys expire
 * and time-scored indexes are trimmed of entries older than the TTL on write.
 */
public final class RedisStorage extends StorageComponent {
  public static Builder newBuilder() {
    return new Builder();
  }

  public static final class Builder extends StorageComponent.Builder {
    String host = "localhost", username, password, keyPrefix = "zipkin:";
    int port = 6379, database, timeout = 2000, maxConnections = 8;
    int ttl = (int) TimeUnit.DAYS.toSeconds(7);
    boolean useSsl, strictTraceId = true, searchEnabled = true;
    List<String> autocompleteKeys = List.of();
    int autocompleteTtl = (int) TimeUnit.HOURS.toMillis(1);
    int autocompleteCardinality = 5 * 4000; // Ex. 5 site tags with cardinality 4000 each

    Builder() {
    }

    /** Redis host. Defaults to "localhost" */
    public Builder host(String host) {
      if (host == null || host.isEmpty()) throw new IllegalArgumentException("host was empty");
      this.host = host;
      return this;
    }

    /** Redis port. Defaults to 6379 */
    public Builder port(int port) {
      if (port <= 0) throw new IllegalArgumentException("port <= 0");
      this.port = port;
      return this;
    }

    /** ACL username for Redis 6+ authentication. Defaults to none. */
    public Builder username(@Nullable String username) {
      this.username = emptyToNull(username);
      return this;
    }

    /** Redis password. Defaults to none. */
    public Builder password(@Nullable String password) {
      this.password = emptyToNull(password);
      return this;
    }

    /** Redis logical database. Defaults to 0 */
    public Builder database(int database) {
      if (database < 0) throw new IllegalArgumentException("database < 0");
      this.database = database;
      return this;
    }

    /** Use TLS when connecting to Redis. Defaults to false */
    public Builder useSsl(boolean useSsl) {
      this.useSsl = useSsl;
      return this;
    }

    /** Connect and socket timeout in milliseconds. Defaults to 2000 */
    public Builder timeout(int timeout) {
      if (timeout <= 0) throw new IllegalArgumentException("timeout <= 0");
      this.timeout = timeout;
      return this;
    }

    /** Maximum concurrent connections to Redis, and threads used for async calls. Defaults to 8 */
    public Builder maxConnections(int maxConnections) {
      if (maxConnections <= 0) throw new IllegalArgumentException("maxConnections <= 0");
      this.maxConnections = maxConnections;
      return this;
    }

    /** Prefix for all keys written by Zipkin. Defaults to "zipkin:" */
    public Builder keyPrefix(String keyPrefix) {
      if (keyPrefix == null) throw new NullPointerException("keyPrefix == null");
      this.keyPrefix = keyPrefix;
      return this;
    }

    /**
     * Time in seconds that spans and index entries are retained. Zero disables expiry, relying on
     * Redis eviction policy instead. Defaults to 7 days.
     */
    public Builder ttl(int ttl) {
      if (ttl < 0) throw new IllegalArgumentException("ttl < 0");
      this.ttl = ttl;
      return this;
    }

    /** {@inheritDoc} */
    @Override public Builder strictTraceId(boolean strictTraceId) {
      this.strictTraceId = strictTraceId;
      return this;
    }

    /** {@inheritDoc} */
    @Override public Builder searchEnabled(boolean searchEnabled) {
      this.searchEnabled = searchEnabled;
      return this;
    }

    /** {@inheritDoc} */
    @Override public Builder autocompleteKeys(List<String> keys) {
      if (keys == null) throw new NullPointerException("keys == null");
      this.autocompleteKeys = List.copyOf(keys);
      return this;
    }

    /** {@inheritDoc} */
    @Override public Builder autocompleteTtl(int autocompleteTtl) {
      if (autocompleteTtl <= 0) throw new IllegalArgumentException("autocompleteTtl <= 0");
      this.autocompleteTtl = autocompleteTtl;
      return this;
    }

    /** {@inheritDoc} */
    @Override public Builder autocompleteCardinality(int autocompleteCardinality) {
      if (autocompleteCardinality <= 0) {
        throw new IllegalArgumentException("autocompleteCardinality <= 0");
      }
      this.autocompleteCardinality = autocompleteCardinality;
      return this;
    }

    @Override public RedisStorage build() {
      return new RedisStorage(this);
    }
  }

  final String host, keyPrefix;
  @Nullable final String username, password;
  final int port, database, timeout, maxConnections, ttl;
  final boolean useSsl, strictTraceId, searchEnabled;
  final List<String> autocompleteKeys;
  final int autocompleteTtl, autocompleteCardinality;
  final RedisKeys keys;
  final ExecutorService executor;

  volatile UnifiedJedis jedis;
  volatile RedisSpanConsumer spanConsumer;
  volatile RedisSpanStore spanStore;
  volatile RedisAutocompleteTags autocompleteTags;
  volatile boolean closeCalled;

  RedisStorage(Builder builder) {
    host = builder.host;
    port = builder.port;
    username = builder.username;
    password = builder.password;
    database = builder.database;
    useSsl = builder.useSsl;
    timeout = builder.timeout;
    maxConnections = builder.maxConnections;
    keyPrefix = builder.keyPrefix;
    ttl = builder.ttl;
    strictTraceId = builder.strictTraceId;
    searchEnabled = builder.searchEnabled;
    autocompleteKeys = builder.autocompleteKeys;
    autocompleteTtl = builder.autocompleteTtl;
    autocompleteCardinality = builder.autocompleteCardinality;
    keys = new RedisKeys(keyPrefix);
    executor = newExecutor(maxConnections);
  }

  /** Lazy to avoid eager I/O */
  UnifiedJedis jedis() {
    if (closeCalled) throw new ClosedComponentException();
    if (jedis == null) {
      synchronized (this) {
        if (closeCalled) throw new ClosedComponentException();
        if (jedis == null) jedis = newJedis();
      }
    }
    return jedis;
  }

  UnifiedJedis newJedis() {
    DefaultJedisClientConfig.Builder config = DefaultJedisClientConfig.builder()
      .timeoutMillis(timeout)
      .database(database)
      .ssl(useSsl)
      .clientName("zipkin");
    if (username != null) config.user(username);
    if (password != null) config.password(password);
    ConnectionPoolConfig poolConfig = new ConnectionPoolConfig();
    poolConfig.setMaxTotal(maxConnections);
    poolConfig.setMaxIdle(maxConnections);
    poolConfig.setMaxWait(java.time.Duration.ofMillis(timeout));
    return new JedisPooled(new HostAndPort(host, port), config.build(), poolConfig);
  }

  @Override public SpanStore spanStore() {
    return spanStoreImpl();
  }

  @Override public Traces traces() {
    return spanStoreImpl();
  }

  @Override public ServiceAndSpanNames serviceAndSpanNames() {
    return spanStoreImpl();
  }

  RedisSpanStore spanStoreImpl() {
    if (spanStore == null) {
      synchronized (this) {
        if (spanStore == null) spanStore = new RedisSpanStore(this);
      }
    }
    return spanStore;
  }

  @Override public AutocompleteTags autocompleteTags() {
    if (autocompleteTags == null) {
      synchronized (this) {
        if (autocompleteTags == null) autocompleteTags = new RedisAutocompleteTags(this);
      }
    }
    return autocompleteTags;
  }

  @Override public SpanConsumer spanConsumer() {
    if (spanConsumer == null) {
      synchronized (this) {
        if (spanConsumer == null) spanConsumer = new RedisSpanConsumer(this);
      }
    }
    return spanConsumer;
  }

  @Override public CheckResult check() {
    try {
      jedis().ping();
    } catch (Throwable e) {
      Call.propagateIfFatal(e);
      return CheckResult.failed(e);
    }
    return CheckResult.OK;
  }

  /**
   * Redis signals overload with "BUSY" (script running) or "OOM" (maxmemory reached with a
   * noeviction policy). Locally, we are over capacity when the connection pool is exhausted or our
   * executor rejects work.
   */
  @Override public boolean isOverCapacity(Throwable e) {
    if (e instanceof RejectedExecutionException || e instanceof JedisBusyException) return true;
    if (e instanceof JedisDataException) {
      String message = e.getMessage();
      return message != null && message.startsWith("OOM");
    }
    return e instanceof JedisException && e.getCause() instanceof NoSuchElementException;
  }

  /** Deletes all keys with the configured prefix. Visible for testing. */
  void clear() {
    UnifiedJedis jedis = jedis();
    ScanParams params = new ScanParams().match(keyPrefix + "*").count(1000);
    String cursor = ScanParams.SCAN_POINTER_START;
    do {
      ScanResult<String> result = jedis.scan(cursor, params);
      List<String> toDelete = new ArrayList<>(result.getResult());
      if (!toDelete.isEmpty()) jedis.del(toDelete.toArray(new String[0]));
      cursor = result.getCursor();
    } while (!ScanParams.SCAN_POINTER_START.equals(cursor));
    RedisSpanConsumer consumer = spanConsumer;
    if (consumer != null) consumer.clear();
  }

  @Override public void close() {
    if (closeCalled) return;
    synchronized (this) {
      if (closeCalled) return;
      closeCalled = true;
      executor.shutdownNow();
      UnifiedJedis jedis = this.jedis;
      if (jedis != null) jedis.close();
    }
  }

  @Override public String toString() {
    return "RedisStorage{host=" + host + ", port=" + port + ", database=" + database
      + ", keyPrefix=" + keyPrefix + "}";
  }

  /** Bounds async backlog so a slow Redis rejects calls instead of exhausting the heap. */
  static final int MAX_QUEUED_CALLS = 1000;

  static ExecutorService newExecutor(int threads) {
    AtomicInteger count = new AtomicInteger();
    ThreadPoolExecutor executor = new ThreadPoolExecutor(threads, threads, 60, TimeUnit.SECONDS,
      new LinkedBlockingQueue<>(MAX_QUEUED_CALLS), r -> {
      Thread thread = new Thread(r, "zipkin-redis-" + count.incrementAndGet());
      thread.setDaemon(true);
      return thread;
    });
    executor.allowCoreThreadTimeOut(true);
    return executor;
  }

  static String emptyToNull(String value) {
    return value == null || value.isEmpty() ? null : value;
  }
}
