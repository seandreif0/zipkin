/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.storage.redis;

import java.util.NoSuchElementException;
import java.util.concurrent.RejectedExecutionException;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.exceptions.JedisBusyException;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.exceptions.JedisException;
import zipkin2.CheckResult;
import zipkin2.internal.ClosedComponentException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RedisStorageTest {
  @Test void isOverCapacity() {
    try (RedisStorage storage = RedisStorage.newBuilder().build()) {
      assertThat(storage.isOverCapacity(new RejectedExecutionException())).isTrue();
      assertThat(storage.isOverCapacity(new JedisBusyException("BUSY Redis is busy"))).isTrue();
      assertThat(storage.isOverCapacity(
        new JedisDataException("OOM command not allowed when used memory > 'maxmemory'.")))
        .isTrue();
      assertThat(storage.isOverCapacity(new JedisException(
        "Could not get a resource from the pool", new NoSuchElementException()))).isTrue();

      assertThat(storage.isOverCapacity(new JedisDataException("WRONGTYPE"))).isFalse();
      assertThat(storage.isOverCapacity(new JedisConnectionException("refused"))).isFalse();
      assertThat(storage.isOverCapacity(new IllegalStateException("Rejected"))).isFalse();
    }
  }

  @Test void check_failsWhenRedisIsDown() {
    try (RedisStorage storage = RedisStorage.newBuilder().host("127.0.0.1").port(1).timeout(100)
      .build()) {
      CheckResult result = storage.check();
      assertThat(result.ok()).isFalse();
      assertThat(result.error()).isInstanceOf(JedisConnectionException.class);
    }
  }

  @Test void check_failsAfterClose() {
    RedisStorage storage = RedisStorage.newBuilder().build();
    storage.close();
    assertThat(storage.check().error()).isInstanceOf(ClosedComponentException.class);
  }

  @Test void builder_validates() {
    RedisStorage.Builder builder = RedisStorage.newBuilder();
    assertThatThrownBy(() -> builder.ttl(-1)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> builder.port(0)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> builder.keyPrefix(null)).isInstanceOf(NullPointerException.class);
  }

  @Test void toString_omitsCredentials() {
    try (RedisStorage storage = RedisStorage.newBuilder().password("secret").build()) {
      assertThat(storage).hasToString(
        "RedisStorage{host=localhost, port=6379, database=0, keyPrefix=zipkin:}");
    }
  }
}
