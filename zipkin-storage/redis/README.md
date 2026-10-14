# storage-redis

This is a Redis 7.2+ storage component, tested against Redis 7.4. It uses the
[Jedis](https://github.com/redis/jedis) client.

`RedisStorage.spanConsumer()` writes spans in a single pipeline per request.
Reads are synchronous on the calling thread, while `Call.enqueue` runs on a
fixed pool sized by `maxConnections`.

## Configuration
```java
storage = RedisStorage.newBuilder()
  .host("localhost")
  .port(6379)
  .password("secret")
  .keyPrefix("zipkin:")
  .ttl((int) TimeUnit.DAYS.toSeconds(7))
  .build();
```

When used in zipkin-server, see the [Redis storage](../../zipkin-server#redis-storage)
section for environment variables.

## Data layout
All keys start with `keyPrefix` (default `zipkin:`), so multiple sites can
share a Redis database.

| Key                                                  | Type | Contents                                            |
|------------------------------------------------------|------|-----------------------------------------------------|
| `trace:<lowTraceId>`                                 | SET  | Zipkin v2 JSON spans for the lower 64-bit trace ID  |
| `idx:ts`                                             | ZSET | `<lowTraceId>:<timestampMicros>` scored by timestamp |
| `idx:service:<service>`                              | ZSET | as above, for spans of a local service              |
| `idx:span:<service>:<spanName>`                      | ZSET | as above, for a service's span name                 |
| `idx:remote:<service>:<remoteService>`               | ZSET | as above, for a service's remote service            |
| `idx:tag:<key>:<value>`                              | ZSET | as above, for `autocompleteKeys` tags               |
| `idx:duration[:service:<service>]:<epochDay>`        | ZSET | `<lowTraceId>:<timestampMicros>` scored by duration |
| `serviceNames`, `spanNames:<service>`, `remoteServiceNames:<service>` | SET | names for the UI          |
| `autocomplete:<key>`                                 | SET  | distinct values of `autocompleteKeys` tags          |

Queries read the most selective index in the time range, then apply
`QueryRequest.test` to the fetched traces. Duration queries scan day buckets
in the time range. Like other backends, traces are grouped by the lower 64
bits of the trace ID, and `strictTraceId(false)` returns mixed-length IDs
together.

When `searchEnabled(false)`, only `trace:*` and `idx:ts` (needed for
dependency links) are written.

Name and autocomplete writes are de-duplicated for `autocompleteTtl`
milliseconds, bounded by `autocompleteCardinality`, as in Cassandra.

## Retention
`ttl` (seconds, default 7 days) sets `EXPIRE` on every key written, and trims
entries older than the TTL from time-scored indexes on each write. Day buckets
for duration expire a day after the TTL. Set `ttl(0)` to disable expiry and
rely on Redis `maxmemory-policy` instead.

## Dependency links
Links are computed on demand with `DependencyLinker` from traces in the
requested window, so no [zipkin-dependencies](https://github.com/openzipkin/zipkin-dependencies)
job is needed.

## Testing
`ITRedisStorage` runs the shared storage contracts in `zipkin-tests` against
a Redis container via Testcontainers. Docker is required.
```bash
$ ./mvnw clean verify -pl zipkin-storage/redis -am
```
