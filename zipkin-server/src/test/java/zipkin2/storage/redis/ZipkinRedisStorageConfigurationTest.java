/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.storage.redis;

import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import zipkin2.server.internal.redis.Access;
import zipkin2.storage.StorageComponent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

class ZipkinRedisStorageConfigurationTest {

  AnnotationConfigApplicationContext context;

  @AfterEach void close() {
    if (context != null) {
      context.close();
    }
  }

  @Test void doesntProvideStorageComponent_whenStorageTypeNotRedis() {
    for (String type : List.of("mem", "mysql", "cassandra3", "elasticsearch")) {
      context = new AnnotationConfigApplicationContext();
      TestPropertyValues.of("zipkin.storage.type:" + type).applyTo(context);
      Access.registerRedis(context);
      context.refresh();

      assertThatExceptionOfType(NoSuchBeanDefinitionException.class)
        .as(type)
        .isThrownBy(() -> context.getBean(StorageComponent.class));
      context.close();
    }
  }

  @Test void doesntProvideStorageComponent_whenStorageTypeUnset() {
    context = new AnnotationConfigApplicationContext();
    Access.registerRedis(context);
    context.refresh();

    assertThatExceptionOfType(NoSuchBeanDefinitionException.class)
      .isThrownBy(() -> context.getBean(StorageComponent.class));
  }

  @Test void providesStorageComponent_whenStorageTypeRedis() {
    context = new AnnotationConfigApplicationContext();
    TestPropertyValues.of("zipkin.storage.type:redis").applyTo(context);
    Access.registerRedis(context);
    context.refresh();

    assertThat(context.getBean(StorageComponent.class)).isInstanceOf(RedisStorage.class);
  }

  @Test void defaults() {
    context = new AnnotationConfigApplicationContext();
    TestPropertyValues.of("zipkin.storage.type:redis").applyTo(context);
    Access.registerRedis(context);
    context.refresh();

    RedisStorage storage = context.getBean(RedisStorage.class);
    assertThat(storage.host).isEqualTo("localhost");
    assertThat(storage.port).isEqualTo(6379);
    assertThat(storage.username).isNull();
    assertThat(storage.password).isNull();
    assertThat(storage.database).isZero();
    assertThat(storage.useSsl).isFalse();
    assertThat(storage.keyPrefix).isEqualTo("zipkin:");
    assertThat(storage.ttl).isEqualTo(604800);
    assertThat(storage.strictTraceId).isTrue();
    assertThat(storage.searchEnabled).isTrue();
    assertThat(storage.autocompleteTtl).isEqualTo(3600000);
    assertThat(storage.autocompleteCardinality).isEqualTo(20000);
  }

  @Test void canOverrideProperties() {
    context = new AnnotationConfigApplicationContext();
    TestPropertyValues.of(
        "zipkin.storage.type:redis",
        "zipkin.storage.redis.host:redis.example.com",
        "zipkin.storage.redis.port:6380",
        "zipkin.storage.redis.username:zipkin",
        "zipkin.storage.redis.password:secret",
        "zipkin.storage.redis.database:2",
        "zipkin.storage.redis.use-ssl:true",
        "zipkin.storage.redis.timeout:500",
        "zipkin.storage.redis.max-connections:16",
        "zipkin.storage.redis.key-prefix:prod:zipkin:",
        "zipkin.storage.redis.ttl:3600")
      .applyTo(context);
    Access.registerRedis(context);
    context.refresh();

    RedisStorage storage = context.getBean(RedisStorage.class);
    assertThat(storage.host).isEqualTo("redis.example.com");
    assertThat(storage.port).isEqualTo(6380);
    assertThat(storage.username).isEqualTo("zipkin");
    assertThat(storage.password).isEqualTo("secret");
    assertThat(storage.database).isEqualTo(2);
    assertThat(storage.useSsl).isTrue();
    assertThat(storage.timeout).isEqualTo(500);
    assertThat(storage.maxConnections).isEqualTo(16);
    assertThat(storage.keyPrefix).isEqualTo("prod:zipkin:");
    assertThat(storage.ttl).isEqualTo(3600);
  }

  @Test void commonStorageProperties() {
    context = new AnnotationConfigApplicationContext();
    TestPropertyValues.of(
        "zipkin.storage.type:redis",
        "zipkin.storage.strict-trace-id:false",
        "zipkin.storage.search-enabled:false",
        "zipkin.storage.autocomplete-keys:environment",
        "zipkin.storage.autocomplete-ttl:60000",
        "zipkin.storage.autocomplete-cardinality:100")
      .applyTo(context);
    Access.registerRedis(context);
    context.refresh();

    RedisStorage storage = context.getBean(RedisStorage.class);
    assertThat(storage.strictTraceId).isFalse();
    assertThat(storage.searchEnabled).isFalse();
    assertThat(storage.autocompleteKeys).containsOnly("environment");
    assertThat(storage.autocompleteTtl).isEqualTo(60000);
    assertThat(storage.autocompleteCardinality).isEqualTo(100);
  }
}
