# Architecture overview

## Module graph

```
                    ┌──────────────────────────────┐
                    │        pgmq-core             │  plain Spring JDBC/TX
                    │  PgmqOperations/PgmqTemplate │  no auto-configuration
                    │  listener container          │  optional: Jackson, Micrometer
                    │  payload conversion          │
                    └──────────────┬───────────────┘
                                   │
                    ┌──────────────┴───────────────┐
                    │ pgmq-spring-boot-autoconfigure│ @AutoConfiguration
                    │  properties, health, metrics  │ optional: spring-boot-health
                    └──────────────┬───────────────┘
                                   │
                    ┌──────────────┴───────────────┐
                    │   pgmq-spring-boot-starter    │
                    │   what apps depend on         │
                    └──────────────────────────────┘
```

Dependencies point one way only. `pgmq-core` knows nothing about Boot.

## Request paths

**Producing**

```
@Transactional method
  └─ PgmqTemplate.send(queue, payload)
       ├─ PayloadConverter.toJson(payload)
       └─ JdbcTemplate ──► DataSourceUtils.getConnection()   ◄── the caller's transaction
                            └─ select * from pgmq.send(?::text, ?::jsonb, ?::jsonb, ?::integer)
```

`DataSourceUtils` is the whole trick: it returns the connection bound to the current Spring
transaction when there is one, which is what makes the send atomic with the caller's own writes.

**Consuming**

```
PgmqMessageListenerContainer            (N polling loops, virtual threads when available)
  └─ poll: pgmq.read(...)  ──► FOR UPDATE SKIP LOCKED, vt += timeout, read_ct += 1
       │                           rows mapped as raw JSON - never converted inside the result set
       ├─ [extendLease] one lease per batch, refreshed until each message is settled
       ├─ read_ct > maxAttempts?  ──► poison: terminal action, handler never runs
       ├─ convert payload         ──► fails? same path as a handler throw, for this message only
       └─ otherwise
            ├─ [transactional] TransactionTemplate {
            │      handler.handle(message)
            │      acknowledge (delete | archive)
            │  }                                    ◄── business writes + ack commit together
            └─ on throw ──► attempts exhausted? terminal action : leave for redelivery
```

## Key design seams

| Seam | Purpose |
|---|---|
| `PayloadConverter` | Swap JSON implementation; default reuses the application's Spring-managed mapper. |
| `PgmqClientListener` | Producer-side observability without a Micrometer dependency in core. |
| `ConsumerListener` | Consumer-side observability and application bookkeeping (e.g. reacting to dead-letters). |
| `PgmqCapabilities` | One catalog probe at startup; every version-dependent branch reads from it. |

## The safety invariant

On any *unexpected* failure — a database error, a listener callback throwing, a bug in the
container — the runtime **touches nothing**. It never deletes or archives a message it is not sure
was handled. PGMQ's visibility timeout then redelivers it.

The cost is a possible duplicate delivery, which at-least-once semantics already require handlers
to tolerate. The alternative would be silent message loss, which they cannot.
