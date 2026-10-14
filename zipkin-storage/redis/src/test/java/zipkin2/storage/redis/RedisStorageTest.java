/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.storage.redis;

import java.util.List;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.JedisPool;
import zipkin2.CheckResult;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RedisStorageTest {

  @Test void build_requiresJedisPool() {
    assertThatThrownBy(() -> RedisStorage.newBuilder().executor(Runnable::run).build())
      .isInstanceOf(NullPointerException.class)
      .hasMessage("jedisPool == null");
  }

  @Test void build_requiresExecutor() {
    try (JedisPool pool = new JedisPool("localhost", 1)) {
      assertThatThrownBy(() -> RedisStorage.newBuilder().jedisPool(pool).build())
        .isInstanceOf(NullPointerException.class)
        .hasMessage("executor == null");
    }
  }

  @Test void ttlSeconds_rejectsNegative() {
    assertThatThrownBy(() -> RedisStorage.newBuilder().ttlSeconds(-1))
      .isInstanceOf(IllegalArgumentException.class);
  }

  @Test void builder_honorsFlags() {
    try (JedisPool pool = new JedisPool("localhost", 1)) {
      RedisStorage storage = RedisStorage.newBuilder()
        .jedisPool(pool)
        .executor(Runnable::run)
        .strictTraceId(false)
        .searchEnabled(false)
        .autocompleteKeys(List.of("environment"))
        .ttlSeconds(60)
        .build();

      assertThat(storage.strictTraceId).isFalse();
      assertThat(storage.searchEnabled).isFalse();
      assertThat(storage.autocompleteKeys).containsExactly("environment");
      assertThat(storage.ttlSeconds).isEqualTo(60);
    }
  }

  @Test void check_failsWhenUnreachable() {
    // port 1 is reserved and nothing should be listening
    try (JedisPool pool = new JedisPool("localhost", 1);
         RedisStorage storage = RedisStorage.newBuilder()
           .jedisPool(pool)
           .executor(Runnable::run)
           .build()) {
      CheckResult result = storage.check();
      assertThat(result.ok()).isFalse();
      assertThat(result.error()).isNotNull();
    }
  }

  @Test void searchDisabled_skipsQueries() throws Exception {
    try (JedisPool pool = new JedisPool("localhost", 1);
         RedisStorage storage = RedisStorage.newBuilder()
           .jedisPool(pool)
           .executor(Runnable::run)
           .searchEnabled(false)
           .build()) {
      // these return without I/O, so the unreachable pool is never used
      assertThat(storage.serviceAndSpanNames().getServiceNames().execute()).isEmpty();
      assertThat(storage.serviceAndSpanNames().getSpanNames("frontend").execute()).isEmpty();
      assertThat(storage.autocompleteTags().getKeys().execute()).isEmpty();
    }
  }
}
