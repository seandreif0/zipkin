/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.storage.redis;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import zipkin2.server.internal.redis.Access;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;

class ZipkinRedisStorageConfigurationTest {

  AnnotationConfigApplicationContext context;

  @AfterEach void close() {
    if (context != null) {
      context.close();
    }
  }

  @Test void doesntProvidesStorageComponent_whenStorageTypeNotRedis() {
    assertThatExceptionOfType(NoSuchBeanDefinitionException.class).isThrownBy(() -> {
      context = new AnnotationConfigApplicationContext();
      TestPropertyValues.of("zipkin.storage.type:cassandra").applyTo(context);
      Access.registerRedis(context);
      context.refresh();
      context.getBean(RedisStorage.class);
    });
  }

  @Test void providesStorageComponent_whenStorageTypeRedis() {
    context = new AnnotationConfigApplicationContext();
    TestPropertyValues.of("zipkin.storage.type:redis").applyTo(context);
    Access.registerRedis(context);
    context.refresh();

    assertThat(context.getBean(RedisStorage.class)).isNotNull();
  }

  @Test void ttl_defaultsToSevenDays() {
    context = new AnnotationConfigApplicationContext();
    TestPropertyValues.of("zipkin.storage.type:redis").applyTo(context);
    Access.registerRedis(context);
    context.refresh();

    assertThat(context.getBean(RedisStorage.class).ttlSeconds).isEqualTo(604800L);
  }

  @Test void canOverridesProperty_ttl() {
    context = new AnnotationConfigApplicationContext();
    TestPropertyValues.of(
        "zipkin.storage.type:redis",
        "zipkin.storage.redis.ttl:3600")
      .applyTo(context);
    Access.registerRedis(context);
    context.refresh();

    assertThat(context.getBean(RedisStorage.class).ttlSeconds).isEqualTo(3600L);
  }

  @Test void strictTraceId_defaultsToTrue() {
    context = new AnnotationConfigApplicationContext();
    TestPropertyValues.of("zipkin.storage.type:redis").applyTo(context);
    Access.registerRedis(context);
    context.refresh();
    assertThat(context.getBean(RedisStorage.class).strictTraceId).isTrue();
  }

  @Test void strictTraceId_canSetToFalse() {
    context = new AnnotationConfigApplicationContext();
    TestPropertyValues.of(
        "zipkin.storage.type:redis",
        "zipkin.storage.strict-trace-id:false")
      .applyTo(context);
    Access.registerRedis(context);
    context.refresh();

    assertThat(context.getBean(RedisStorage.class).strictTraceId).isFalse();
  }

  @Test void searchEnabled_canSetToFalse() {
    context = new AnnotationConfigApplicationContext();
    TestPropertyValues.of(
      "zipkin.storage.type:redis",
      "zipkin.storage.search-enabled:false")
      .applyTo(context);
    Access.registerRedis(context);
    context.refresh();

    assertThat(context.getBean(RedisStorage.class).searchEnabled).isFalse();
  }

  @Test void autocompleteKeys_list() {
    context = new AnnotationConfigApplicationContext();
    TestPropertyValues.of(
      "zipkin.storage.type:redis",
      "zipkin.storage.autocomplete-keys:environment")
      .applyTo(context);
    Access.registerRedis(context);
    context.refresh();

    assertThat(context.getBean(RedisStorage.class).autocompleteKeys)
      .containsOnly("environment");
  }
}
