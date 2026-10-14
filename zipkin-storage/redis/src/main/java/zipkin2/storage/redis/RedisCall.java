/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.storage.redis;

import java.util.concurrent.Executor;
import java.util.function.Function;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import zipkin2.Call;
import zipkin2.Callback;

/** Uncancelable call that borrows a pooled connection, enqueued on an executor. */
final class RedisCall<V> extends Call.Base<V> {

  static final class Factory {
    final JedisPool jedisPool;
    final Executor executor;

    Factory(JedisPool jedisPool, Executor executor) {
      this.jedisPool = jedisPool;
      this.executor = executor;
    }

    <V> RedisCall<V> create(Function<Jedis, V> function) {
      return new RedisCall<>(this, function);
    }
  }

  final Factory factory;
  final Function<Jedis, V> function;

  RedisCall(Factory factory, Function<Jedis, V> function) {
    this.factory = factory;
    this.function = function;
  }

  @Override protected V doExecute() {
    try (Jedis jedis = factory.jedisPool.getResource()) {
      return function.apply(jedis);
    }
  }

  @Override protected void doEnqueue(Callback<V> callback) {
    factory.executor.execute(() -> {
      try {
        callback.onSuccess(doExecute());
      } catch (Throwable t) {
        propagateIfFatal(t);
        callback.onError(t);
      }
    });
  }

  @Override public String toString() {
    return function.toString();
  }

  @Override public Call<V> clone() {
    return new RedisCall<>(factory, function);
  }
}
