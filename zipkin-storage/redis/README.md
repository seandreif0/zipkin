# storage-redis

This is a Redis 7+ storage component, using the [Jedis](https://github.com/redis/jedis) client.

`RedisStorage.Builder` requires a `JedisPool` and an `Executor`. The pool is owned by the caller
and is not closed when the storage component is closed.

```java
storage = RedisStorage.newBuilder()
  .jedisPool(new JedisPool("localhost", 6379))
  .executor(executor)
  .ttlSeconds(TimeUnit.DAYS.toSeconds(7))
  .build();
```

## Data layout
Spans are encoded as JSON v2 and stored in a set per lower 64-bits of the trace ID, so re-reported
identical spans are deduplicated and `strictTraceId(false)` can group mixed-length trace IDs.

Secondary indexes are sorted sets scored by span timestamp (epoch millis). Members are
`lowTraceId:timestampMillis`, so a trace with spans at different times is found by any of them.

| Key                                    | Type | Purpose                                 |
|----------------------------------------|------|-----------------------------------------|
| `zipkin:trace:<lowTraceId>`            | SET  | JSON v2 spans in the trace              |
| `zipkin:index:traces`                  | ZSET | all traces, used for dependency links   |
| `zipkin:index:service:<serviceName>`   | ZSET | traces by local service name            |
| `zipkin:services`                      | SET  | local service names                     |
| `zipkin:span_names:<serviceName>`      | SET  | span names by service                   |
| `zipkin:remote_services:<serviceName>` | SET  | remote service names by service         |
| `zipkin:autocomplete:<tagKey>`         | SET  | values of autocomplete tag keys         |

When `searchEnabled(false)`, only the trace sets and `zipkin:index:traces` are written.

Every key written is given the configured TTL (default 7 days) on each write. Index entries whose
trace has expired are skipped when read.

`getTraces(QueryRequest)` reads index entries newest first in the requested time window and then
filters traces in memory with `QueryRequest.test`. Dependency links are aggregated on read.

## Testing
This module conditionally runs integration tests against a local Redis container.

```bash
$ ./mvnw clean install -pl :zipkin-storage-redis
```
