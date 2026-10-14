/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.storage.redis;

/**
 * Key layout. All keys share a configurable prefix so that multiple Zipkin deployments can share a
 * Redis database.
 *
 * <ul>
 *   <li>{@code trace:<lowTraceId>} SET of JSON-encoded spans</li>
 *   <li>{@code idx:ts} ZSET of {@code <lowTraceId>:<timestampMicros>} scored by timestamp</li>
 *   <li>{@code idx:service|span|remote|tag:...} ZSETs like {@code idx:ts}, scoped to a criteria
 *   </li>
 *   <li>{@code idx:duration[:service:<name>]:<epochDay>} ZSET scored by duration</li>
 *   <li>{@code serviceNames}, {@code spanNames:<service>}, {@code remoteServiceNames:<service>},
 *   {@code autocomplete:<key>} SETs of names</li>
 * </ul>
 */
final class RedisKeys {
  final String prefix;

  RedisKeys(String prefix) {
    this.prefix = prefix;
  }

  String trace(String lowTraceId) {
    return prefix + "trace:" + lowTraceId;
  }

  String timestampIndex() {
    return prefix + "idx:ts";
  }

  String serviceIndex(String serviceName) {
    return prefix + "idx:service:" + serviceName;
  }

  String spanIndex(String serviceName, String spanName) {
    return prefix + "idx:span:" + serviceName + ":" + spanName;
  }

  String remoteServiceIndex(String serviceName, String remoteServiceName) {
    return prefix + "idx:remote:" + serviceName + ":" + remoteServiceName;
  }

  String tagIndex(String key, String value) {
    return prefix + "idx:tag:" + key + ":" + value;
  }

  String durationIndex(String serviceName, long epochDay) {
    if (serviceName == null) return prefix + "idx:duration:" + epochDay;
    return prefix + "idx:duration:service:" + serviceName + ":" + epochDay;
  }

  String serviceNames() {
    return prefix + "serviceNames";
  }

  String spanNames(String serviceName) {
    return prefix + "spanNames:" + serviceName;
  }

  String remoteServiceNames(String serviceName) {
    return prefix + "remoteServiceNames:" + serviceName;
  }

  String autocompleteValues(String key) {
    return prefix + "autocomplete:" + key;
  }
}
