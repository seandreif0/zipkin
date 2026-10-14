/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.storage.redis;

import java.util.concurrent.RejectedExecutionException;
import java.util.function.Function;
import redis.clients.jedis.UnifiedJedis;
import zipkin2.Call;
import zipkin2.Callback;

/** Runs a Redis operation synchronously or on the storage executor. */
final class RedisCall<V> extends Call.Base<V> {
  final RedisStorage storage;
  final String name;
  final Function<UnifiedJedis, V> operation;

  RedisCall(RedisStorage storage, String name, Function<UnifiedJedis, V> operation) {
    this.storage = storage;
    this.name = name;
    this.operation = operation;
  }

  @Override protected V doExecute() {
    return operation.apply(storage.jedis());
  }

  @Override protected void doEnqueue(Callback<V> callback) {
    try {
      storage.executor.execute(() -> {
        V result;
        try {
          result = doExecute();
        } catch (Throwable t) {
          propagateIfFatal(t);
          callback.onError(t);
          return;
        }
        callback.onSuccess(result);
      });
    } catch (RejectedExecutionException e) {
      callback.onError(e);
    }
  }

  @Override public Call<V> clone() {
    return new RedisCall<>(storage, name, operation);
  }

  @Override public String toString() {
    return "RedisCall{" + name + "}";
  }
}
