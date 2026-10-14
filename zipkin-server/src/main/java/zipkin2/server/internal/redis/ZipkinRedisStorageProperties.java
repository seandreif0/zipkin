/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.server.internal.redis;

import org.springframework.boot.context.properties.ConfigurationProperties;
import zipkin2.storage.redis.RedisStorage;

@ConfigurationProperties("zipkin.storage.redis")
class ZipkinRedisStorageProperties {
  private String host = "localhost";
  private int port = 6379;
  private String username;
  private String password;
  private int database;
  private boolean useSsl;
  private int timeout = 2000;
  private int maxConnections = 8;
  private String keyPrefix = "zipkin:";
  private int ttl = 604800;

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
    this.username = username;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public int getDatabase() {
    return database;
  }

  public void setDatabase(int database) {
    this.database = database;
  }

  public boolean isUseSsl() {
    return useSsl;
  }

  public void setUseSsl(boolean useSsl) {
    this.useSsl = useSsl;
  }

  public int getTimeout() {
    return timeout;
  }

  public void setTimeout(int timeout) {
    this.timeout = timeout;
  }

  public int getMaxConnections() {
    return maxConnections;
  }

  public void setMaxConnections(int maxConnections) {
    this.maxConnections = maxConnections;
  }

  public String getKeyPrefix() {
    return keyPrefix;
  }

  public void setKeyPrefix(String keyPrefix) {
    this.keyPrefix = keyPrefix;
  }

  public int getTtl() {
    return ttl;
  }

  public void setTtl(int ttl) {
    this.ttl = ttl;
  }

  RedisStorage.Builder toBuilder() {
    return RedisStorage.newBuilder()
      .host(host)
      .port(port)
      .username(username)
      .password(password)
      .database(database)
      .useSsl(useSsl)
      .timeout(timeout)
      .maxConnections(maxConnections)
      .keyPrefix(keyPrefix != null ? keyPrefix : "")
      .ttl(ttl);
  }
}
