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
  final boolean enabled;

  RedisAutocompleteTags(RedisStorage storage) {
    this.storage = storage;
    this.enabled = storage.searchEnabled && !storage.autocompleteKeys.isEmpty();
  }

  @Override public Call<List<String>> getKeys() {
    if (!enabled) return Call.emptyList();
    return Call.create(storage.autocompleteKeys);
  }

  @Override public Call<List<String>> getValues(String key) {
    if (key == null) throw new NullPointerException("key == null");
    if (key.isEmpty()) throw new IllegalArgumentException("key was empty");
    if (!enabled || !storage.autocompleteKeys.contains(key)) return Call.emptyList();
    String redisKey = storage.keys.autocompleteValues(key);
    return new RedisCall<>(storage, "getValues", jedis -> sorted(jedis.smembers(redisKey)));
  }

  static List<String> sorted(java.util.Collection<String> values) {
    if (values.isEmpty()) return Collections.emptyList();
    List<String> result = new ArrayList<>(values);
    Collections.sort(result);
    return result;
  }
}
