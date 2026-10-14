/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.storage.redis;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import zipkin2.Call;
import zipkin2.storage.AutocompleteTags;

final class RedisAutocompleteTags implements AutocompleteTags {
  final RedisStorage storage;
  final Call<List<String>> keysCall;

  RedisAutocompleteTags(RedisStorage storage) {
    this.storage = storage;
    this.keysCall = Call.create(storage.autocompleteKeys);
  }

  @Override public Call<List<String>> getKeys() {
    if (!storage.searchEnabled) return Call.emptyList();
    return keysCall.clone();
  }

  @Override public Call<List<String>> getValues(String key) {
    if (key == null) throw new NullPointerException("key == null");
    if (key.isEmpty()) throw new IllegalArgumentException("key was empty");
    if (!storage.searchEnabled || !storage.autocompleteKeys.contains(key)) {
      return Call.emptyList();
    }
    String redisKey = RedisKeys.autocomplete(key);
    return storage.callFactory.create(jedis -> {
      List<String> result = new ArrayList<>(jedis.smembers(redisKey));
      Collections.sort(result);
      return result;
    });
  }

  @Override public String toString() {
    return "RedisAutocompleteTags{" + storage + "}";
  }
}
