/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.storage.redis;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import redis.clients.jedis.JedisPool;
import zipkin2.CheckResult;

import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.testcontainers.utility.DockerImageName.parse;

class RedisExtension implements BeforeAllCallback, AfterAllCallback {
  static final Logger LOGGER = LoggerFactory.getLogger(RedisExtension.class);

  final RedisContainer container = new RedisContainer();
  JedisPool jedisPool;

  @Override public void beforeAll(ExtensionContext context) {
    if (context.getRequiredTestClass().getEnclosingClass() != null) {
      // Only run once in outermost scope.
      return;
    }

    container.start();
    LOGGER.info("Using hostPort {}:{}", host(), port());
    jedisPool = new JedisPool(host(), port());

    try (RedisStorage result = computeStorageBuilder().build()) {
      CheckResult check = result.check();
      assumeTrue(check.ok(), () -> "Could not connect to storage, skipping test: "
        + check.error().getMessage());
      result.clear();
    }
  }

  @Override public void afterAll(ExtensionContext context) {
    if (context.getRequiredTestClass().getEnclosingClass() != null) {
      // Only run once in outermost scope.
      return;
    }

    if (jedisPool != null) jedisPool.close();
    container.stop();
  }

  RedisStorage.Builder computeStorageBuilder() {
    return RedisStorage.newBuilder()
      .jedisPool(jedisPool)
      .executor(Runnable::run);
  }

  String host() {
    return container.getHost();
  }

  int port() {
    return container.getMappedPort(6379);
  }

  static final class RedisContainer extends GenericContainer<RedisContainer> {
    RedisContainer() {
      super(parse("redis:7.4-alpine"));
      addExposedPort(6379);
      waitStrategy = Wait.forLogMessage(".*Ready to accept connections.*", 1);
      withLogConsumer(new Slf4jLogConsumer(LOGGER));
    }
  }
}
