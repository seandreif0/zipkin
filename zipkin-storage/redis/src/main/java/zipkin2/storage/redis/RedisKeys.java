/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.storage.redis;

/**
 * Key layout. Index members are {@code lowTraceId:timestampMillis} so a trace with spans at
 * different times is found by any of them; scores are the span timestamp in epoch millis.
 *
 * <pre>{@code
 * zipkin:trace:<lowTraceId>              SET  of JSON v2 encoded spans
 * zipkin:index:traces                    ZSET of <lowTraceId>:<millis>, for dependency links
 * zipkin:index:service:<serviceName>     ZSET of <lowTraceId>:<millis>, for trace search
 * zipkin:services                        SET  of local service names
 * zipkin:span_names:<serviceName>        SET  of span names
 * zipkin:remote_services:<serviceName>   SET  of remote service names
 * zipkin:autocomplete:<tagKey>           SET  of tag values
 * }</pre>
 */
final class RedisKeys {
  static final String PREFIX = "zipkin:";
  static final String TRACE_INDEX = PREFIX + "index:traces";
  static final String SERVICES = PREFIX + "services";

  static String trace(String lowTraceId) {
    return PREFIX + "trace:" + lowTraceId;
  }

  static String serviceIndex(String serviceName) {
    return PREFIX + "index:service:" + serviceName;
  }

  static String spanNames(String serviceName) {
    return PREFIX + "span_names:" + serviceName;
  }

  static String remoteServiceNames(String serviceName) {
    return PREFIX + "remote_services:" + serviceName;
  }

  static String autocomplete(String tagKey) {
    return PREFIX + "autocomplete:" + tagKey;
  }

  static String indexMember(String lowTraceId, long timestampMillis) {
    return lowTraceId + ":" + timestampMillis;
  }

  static String lowTraceIdFromIndexMember(String member) {
    return member.substring(0, member.indexOf(':'));
  }

  static String lowTraceId(String traceId) {
    return traceId.length() == 32 ? traceId.substring(16) : traceId;
  }

  RedisKeys() {
  }
}
