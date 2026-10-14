/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.server.internal.redis;

import java.util.List;
import java.util.concurrent.Executor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import redis.clients.jedis.JedisPool;
import zipkin2.storage.StorageComponent;
import zipkin2.storage.redis.RedisStorage;

@EnableConfigurationProperties(ZipkinRedisStorageProperties.class)
@ConditionalOnClass(RedisStorage.class)
@ConditionalOnProperty(name = "zipkin.storage.type", havingValue = "redis")
@ConditionalOnMissingBean(StorageComponent.class)
public class ZipkinRedisStorageConfiguration {
  @Autowired(required = false) ZipkinRedisStorageProperties redis;

  @Bean @ConditionalOnMissingBean
  Executor redisExecutor() {
    ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
    executor.setThreadNamePrefix("ZipkinRedisStorage-");
    executor.initialize();
    return executor;
  }

  @Bean(destroyMethod = "close") @ConditionalOnMissingBean
  JedisPool redisJedisPool() {
    return redis.toJedisPool();
  }

  @Bean StorageComponent storage(
    Executor redisExecutor,
    JedisPool redisJedisPool,
    @Value("${zipkin.storage.strict-trace-id:true}") boolean strictTraceId,
    @Value("${zipkin.storage.search-enabled:true}") boolean searchEnabled,
    @Value("${zipkin.storage.autocomplete-keys:}") List<String> autocompleteKeys) {
    return RedisStorage.newBuilder()
      .strictTraceId(strictTraceId)
      .searchEnabled(searchEnabled)
      .autocompleteKeys(autocompleteKeys)
      .executor(redisExecutor)
      .jedisPool(redisJedisPool)
      .ttlSeconds(redis.getTtl())
      .build();
  }
}
