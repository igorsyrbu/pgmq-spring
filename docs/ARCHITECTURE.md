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
                    │ properties, health, metrics,  │ optional: spring-boot-health
                    │ declared consumers            │
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

That is the default `ConsumeMode.READ`. Four options change the path, each independent of the
others where the option validation allows:

- **Consume modes.** `TRANSACTIONAL_POP` replaces the read and the acknowledgement with one
  `pgmq.pop` inside the handler's transaction: the message is removed exactly when the handler's
  writes commit, and a row lock rather than a lease holds it meanwhile, so one write per message
  and no `read_ct` to count. `POP` pops in its own statement before the handler runs, which is
  at-most-once.
- **Wake-ups.** With `WakeUp.NOTIFY`, `InsertNotificationListener` holds one pooled connection that
  `LISTEN`s on the queue's insert channel and reconnects with backoff. Each notification, and
  every reconnect, signals the container's `WakeUpSignal`, which the polling loops wait on instead
  of sleeping out `pollDelay`; the regular poll stays on as a slow fallback. A pause, a resume, a
  stop and a retry that has become due signal it too.
- **Batch acknowledgements.** With `batchAcknowledgements`, settlements of one polled batch are
  collected and sent as a single multi-id `delete` or `archive` (flushed early at `ackBatchSize`).
  Until the statement is sent the messages stay leased, and a failed flush leaves them to be
  redelivered rather than counting as handler failures.
- **Declared consumers.** `PgmqDeclaredConsumersRegistrar` registers a container bean named
  `pgmqConsumer-<name>` for every entry under `pgmq.consumers`, binding `pgmq.consumer.*` first and
  the entry's own keys on top. The handler is an ordinary bean named by the entry.

## Key design seams

| Seam | Purpose |
|---|---|
| `PayloadConverter` | Swap JSON implementation; default reuses the application's Spring-managed mapper. |
| `PgmqClientListener` | Producer-side observability without a Micrometer dependency in core. |
| `ConsumerListener` | Consumer-side observability and application bookkeeping (e.g. reacting to dead-letters). |
| `PgmqCapabilities` | One catalog probe, run on first use (and at startup when `verify-on-startup` is on); every version-dependent branch reads from it. |
| `ConsumeMode` | How a container takes messages off the queue - leased read, transactional pop, or plain pop - and so what delivery it guarantees. |
| `WakeUp` | How polling loops learn of new messages: polling alone, or also on insert notifications. |
| `InsertNotificationListener` | The `LISTEN` connection behind `WakeUp.NOTIFY`; signals the container's `WakeUpSignal`. Package-private, as is the signal. |

## The safety invariant

On any *unexpected* failure — a database error, a listener callback throwing, a bug in the
container — the runtime **touches nothing**. It never deletes or archives a message it is not sure
was handled. PGMQ's visibility timeout then redelivers it.

The cost is a possible duplicate delivery, which at-least-once semantics already require handlers
to tolerate. The alternative would be silent message loss, which they cannot.
