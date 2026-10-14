/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.server.internal.redis;

import java.io.Serializable;
import org.springframework.boot.context.properties.ConfigurationProperties;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

@ConfigurationProperties("zipkin.storage.redis")
class ZipkinRedisStorageProperties implements Serializable { // for Spark jobs
  private static final long serialVersionUID = 0L;

  private String host = "localhost";
  private int port = 6379;
  private String username;
  private String password;
  private int db = 0;
  /** Seconds to retain trace data after its last write. Zero disables expiry. Defaults to 7 days */
  private long ttl = 604800;
  private int timeout = 2000;
  private int maxActive = 8;
  private int maxIdle = 8;
  private int minIdle = 0;
  private boolean useSsl;

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = "".equals(username) ? null : username;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = "".equals(password) ? null : password;
  }

  public int getDb() {
    return db;
  }

  public void setDb(int db) {
    this.db = db;
  }

  public long getTtl() {
    return ttl;
  }

  public void setTtl(long ttl) {
    this.ttl = ttl;
  }

  public int getTimeout() {
    return timeout;
  }

  public void setTimeout(int timeout) {
    this.timeout = timeout;
  }

  public int getMaxActive() {
    return maxActive;
  }

  public void setMaxActive(int maxActive) {
    this.maxActive = maxActive;
  }

  public int getMaxIdle() {
    return maxIdle;
  }

  public void setMaxIdle(int maxIdle) {
    this.maxIdle = maxIdle;
  }

  public int getMinIdle() {
    return minIdle;
  }

  public void setMinIdle(int minIdle) {
    this.minIdle = minIdle;
  }

  public boolean isUseSsl() {
    return useSsl;
  }

  public void setUseSsl(boolean useSsl) {
    this.useSsl = useSsl;
  }

  public JedisPool toJedisPool() {
    JedisPoolConfig poolConfig = new JedisPoolConfig();
    poolConfig.setMaxTotal(getMaxActive());
    poolConfig.setMaxIdle(getMaxIdle());
    poolConfig.setMinIdle(getMinIdle());
    DefaultJedisClientConfig clientConfig = DefaultJedisClientConfig.builder()
      .user(getUsername())
      .password(getPassword())
      .database(getDb())
      .timeoutMillis(getTimeout())
      .ssl(isUseSsl())
      .build();
    return new JedisPool(poolConfig, new HostAndPort(getHost(), getPort()), clientConfig);
  }
}
