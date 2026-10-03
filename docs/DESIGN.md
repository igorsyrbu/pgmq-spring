# Design notes

The facts the implementation depends on, the decisions built on them, what is out of scope, and
the known limitations. For the component structure see [ARCHITECTURE.md](ARCHITECTURE.md); for
every setting see [CONFIGURATION.md](CONFIGURATION.md).

---

## 1. Platform baseline

| | Value |
|---|---|
| Spring Boot | 4.0.x (compile baseline) and 4.1.x (tested) |
| Spring Framework | 7.0.x, managed identically by both Boot minors |
| Java | 17 baseline; tested on 17, 21 and 25 |
| Jackson | 3 (`tools.jackson`, Boot's default), with Jackson 2 supported as a fallback |
| PGMQ | 1.5.0 minimum; tested on 1.5.1 and 1.13.0 |

The library compiles against the **oldest** supported Boot minor so that one jar runs on all of
them. Spring Boot 3.x is out of scope: it sits on a different Framework generation and is out of
open-source support.

### Boot 4's modular auto-configuration

Boot 4 splits auto-configuration into per-technology modules under
`org.springframework.boot.<technology>.autoconfigure`. The ones this project touches:

| Module | Used for |
|---|---|
| `spring-boot-jdbc` | `DataSourceAutoConfiguration`, ordered before ours |
| `spring-boot-transaction` | `TransactionAutoConfiguration`, ordered before ours |
| `spring-boot-health` | `HealthIndicator`, available **without** the full actuator |
| `spring-boot-micrometer-metrics` | Micrometer registry auto-configuration, referenced by name |

`@AutoConfiguration`, the `@ConditionalOn*` family and the
`META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports` registration
file stay in `spring-boot-autoconfigure`.

---

## 2. The PGMQ SQL contract

Established by introspecting `pg_proc` and `pg_type` in the official images
(`ghcr.io/pgmq/pg{14..18}-pgmq`, multi-arch including `linux/arm64`), not from prose
documentation.

### `message_record` varies by version

| PGMQ | `pgmq.message_record` columns |
|---|---|
| 1.5.x | `msg_id, read_ct, enqueued_at, vt, message, headers` |
| 1.10.0+ | + `last_read_at` |

Reads therefore `select *` and map **by column name**, tolerating absent columns. `metrics_result`
varies the same way (`default_partition_length` appears in later versions). The 1.5.0 minimum is
the release that added headers, which the client exposes as core API.

### Overloads are ambiguous from JDBC

`pgmq.send` has six overloads that differ only in the type of the third argument:

```
send(text, jsonb)                       send(text, jsonb, jsonb)              -- headers
send(text, jsonb, integer)              send(text, jsonb, timestamptz)        -- delay
send(text, jsonb, jsonb, integer)       send(text, jsonb, jsonb, timestamptz)
```

Untyped JDBC placeholders cannot resolve them:

```
ERROR:  function pgmq.send(unknown, unknown, unknown) is not unique
```

Every statement in `PgmqTemplate` targets exactly one overload with an explicit cast on every
parameter. `send_batch`, `set_vt` and the grouped reads are handled the same way.

### Arrays without driver-specific code

Batch payloads and headers travel as one JSON array parameter, expanded server-side. Payloads are
sent as an array of JSON **strings**, each parsed on its own:

```sql
(select array_agg((value #>> '{}')::jsonb order by ord)
   from jsonb_array_elements(?::jsonb) with ordinality as t(value, ord))
```

Splicing the documents' text into one array literal instead would let a malformed document such
as `{"a":1},{"b":2}` become two messages, misaligning the returned ids and per-message headers;
parsed individually, it fails the batch just as `send()` would reject it. `with ordinality` makes
the order explicit rather than relying on scan order. Headers, which the client serializes itself,
are sent as a plain JSON array; JSON `null` elements become SQL `NULL`, so a message without
headers in a batch is stored exactly like one sent alone. Message ids for batch delete, archive and `set_vt` are rendered as a
`'{1,2,3}'::bigint[]` literal, which is injection-safe because the values are `long`s.

### Concurrency, attempts and names

`pgmq.read()` selects `ORDER BY msg_id LIMIT n FOR UPDATE SKIP LOCKED`, then updates `vt`,
`read_ct` and `last_read_at`. That makes competing consumers safe and `read_ct` a reliable attempt
counter that survives restarts and is shared across instances.

Queue names are limited to **47** characters (`template_pgmq_q_` is 16, and 16 + 47 is Postgres's
63-byte identifier limit), must not contain `$`, `;`, `--` or `'`, and are **lower-cased** by
PGMQ - so `Orders` and `orders` are the same queue. `QueueNames` mirrors these rules and exposes
`normalize()` to detect such collisions.

### Chunked batches stay atomic

With `maxBatchSize`, a long batch becomes several `send_batch` statements. A single statement is
all-or-nothing, and splitting it must not quietly change that: a failure in the third chunk after
two committed ones would leave the caller with an exception, no ids, and messages it cannot
identify to resend or clean up. So the chunks join the transaction bound to the client's
`DataSource` when there is one, and otherwise run in a transaction the client opens with a
`DataSourceTransactionManager` of its own. Whether a transaction is bound is decided by
`TransactionSynchronizationManager.hasResource(dataSource)`, not by "is any transaction active":
a transaction on another data source, or a JPA transaction manager that exposes a connection
without marking it active, must not make the client either skip its own transaction or try to
start a nested one on an already-bound connection.

### Whole seconds

Visibility timeouts, delays and long-poll windows are `integer` seconds. The client rounds **up**
and saturates at `Integer.MAX_VALUE`: truncating a 500 ms visibility timeout to 0 would make a
message visible to every consumer the instant it is read.

### `set_vt` with an instant

`set_vt` gained its `timestamptz` overload after 1.5. Where it is missing, `setVisibleAt` (and so
`Acknowledgement.retryAt`) passes the integer overload a delay computed in the database -
`ceil(extract(epoch from (target - clock_timestamp())))` - so the database's clock stays the
reference and the result is exact to the second.

### Dropping a queue drops its archive

`detach_archive()` cannot preserve an archive on every supported version: it is a no-op in recent
PGMQ, and in older releases it makes the following `drop_queue()` fail. The client therefore does
not offer it; `dropQueue` documents that archived messages go with the queue.

### Installing the extension

PGMQ's control file sets `superuser = false`, so any role with `CREATE` on the database - normally
its owner - can run `CREATE EXTENSION pgmq`. The initializer therefore creates it when the `pgmq`
schema is missing (`pgmq.create-extension`, default on): containers and development databases ship
the extension without creating it, and failing there only to ask for one SQL statement helps
nobody. It checks for the schema first, so where PGMQ is installed nothing runs, and a role without
the privilege is never asked for it. A failure is re-checked, as with queue creation, so instances
starting together cannot fail on it.

### Version detection

PGMQ has no `version()` function, and a SQL-only installation has no `pg_extension` row.
Capabilities are therefore probed from the catalog - which functions exist, which columns
`message_record` has - once, then cached. `verifyInstallation` compares the extension version
when there is one and otherwise requires headers.

---

## 3. Decisions

### Failure action is terminal

A failed message is retried until `read_ct` reaches `maxAttempts`; only then is the
`failureAction` applied. Applying it on the first failure would make `maxAttempts` meaningless.
The one exception is a type listed in `nonRetryableExceptions`, matched anywhere in the cause
chain: a retry cannot fix it, so the terminal action is applied at once rather than after every
attempt has been spent - which, with grouped reads, would also hold back the message's whole group.
`REDELIVER` names no terminal state, so an exhausted or poisoned message under it is archived with
an error log rather than looping for ever.

### Retry backoff comes from `read_ct`

With `retryMultiplier`, the delay after a failed delivery is
`retryDelay × retryMultiplier^(read_ct - 1)`, capped at `maxRetryDelay`. Deriving the exponent
from `read_ct` rather than from a counter in the consumer makes the backoff survive restarts and
agree across instances, with no bookkeeping. The arithmetic is done in floating point and
saturates at PGMQ's largest visibility timeout, because `Duration.multipliedBy` would throw on
overflow. An unset `maxRetryDelay` means uncapped: retries only happen while
`read_ct < maxAttempts`, so the attempt limit already bounds the largest delay.

### The container never guesses

On an unexpected failure - a database error, a listener callback throwing, a failed settlement -
the container leaves the message untouched and lets its visibility timeout redeliver it. The cost
is a possible duplicate, which at-least-once delivery already requires handlers to tolerate; the
alternative is silent loss. Following from that:

- **Conversion happens after the read, per message.** Rows are mapped as raw JSON and converted
  one by one, so a payload that cannot be converted fails only that message - which then counts
  towards `maxAttempts` - instead of failing a poll whose whole batch PGMQ has already leased.
- **A settlement counts only once it has succeeded.** If an `Acknowledgement` call throws, the
  message falls through to the failure action.
- **A handler that settles and then throws** keeps its settlement outside a transaction; the
  failure action is not applied on top of it. Inside a transaction the rollback undoes the
  settlement, and the failure action applies.
- **One failure never aborts its batch-mates.** Poison handling and failure actions catch and log.

### Transaction timeouts are Spring's

`transactionTimeout` sets the timeout of the `TransactionTemplate` that runs a transactional
handler, and nothing else: dead-lettering keeps its own, untimed transaction. Spring enforces it
at statement boundaries - `JdbcTemplate` fails a statement that starts after the deadline and
gives a running one the remaining time as its query timeout - which also covers the container's
own acknowledgement, so a handler that overran never commits. Issuing
`SET LOCAL idle_in_transaction_session_timeout` as well was considered and rejected: it costs a
round trip per message, terminates the whole session rather than rolling back, and needs a
connection the container does not otherwise have.

### Notifications wake the loops; polling stays the source of truth

With `wakeUp=NOTIFY` each container holds one connection that `LISTEN`s on
`pgmq.q_<queue>.INSERT`, PGMQ's channel for its insert trigger, and every notification wakes all
of the container's polling loops. A notification only says "look now": the loops still read with
`pgmq.read`, and the regular poll stays on as a fallback, because notifications are lost in three
ways - while the listening connection is being re-established, for messages that become visible
without an insert (a delayed send, a retry, a released lease), and when PGMQ's per-queue throttle
drops them. That throttle discards notifications within its interval instead of deferring them, so
after every wake-up a loop restarts its backoff from `pollDelay`, polling a few times at short
intervals before slowing down again. Retries the container itself schedules wake it when due.

Waits between polls are on a generation counter rather than a sleep: a loop records the generation
before it polls and waits for a later one, so a notification arriving during a poll is never
missed. Stopping and resuming signal it too, so a container waiting out a long fallback poll stops
at once.

The notifications are read through the driver's `PGConnection`, which the pool does not see. A
failure there would leave the pool believing the connection healthy, so before returning it the
listener runs `unlisten *` through the pool's own proxy: on a broken connection that fails where
the pool notices, and HikariCP evicts it instead of lending it out again. The PostgreSQL driver is
a `compileOnly` dependency, checked when a `NOTIFY` container is built. Queue names are passed to
`enable_notify_insert` lower-cased, because PGMQ stores the name as given but its trigger looks the
throttle up by the lower-cased table name, so a mixed-case name would never notify.

### Leases cover the batch

With `extendLease`, one lease covers every message of a polled batch from the read until each is
settled - including messages waiting behind a slow one, and a batch handler's whole batch. Refresh
and settlement take the same lock, so once a message is settled no refresh for it is in flight
or will start; a refresh can never overwrite a retry delay the container has just set. The lock is
a `ReentrantLock` rather than a monitor because a refresh holds it across a database call, which
on JDK 21-23 would pin a virtual thread's carrier. Each polling loop has its own refresh thread, so
one slow refresh never delays another batch's.

### Batched acknowledgements settle at the flush

With `batchAcknowledgements`, a single-message handler's successful messages are collected and
deleted (or archived) with one statement when the polled batch is done, or earlier at
`ackBatchSize`, and always before the next poll. A pending message is settled on its lease only
when the flush is sent, not when its handler returns: settling earlier would stop refreshing it,
and a lease that lapsed before the flush would hand an already-handled message to another
consumer. A failed flush is an unexpected failure like any other - nothing is touched, and the
messages are redelivered when their lease expires. Listeners still see `onSuccess` when the
handler returns. The option is rejected with `transactional`, where the acknowledgement must
commit with the handler's writes, and with `MANUAL`.

### Grouped reads: never concurrent, always in order - not partition ownership

`read_grouped*` withholds a whole group while one of its messages is unacknowledged, so no two
consumers hold unacknowledged messages for the same key, and a key's messages arrive in send
order. Nothing pins a key to an instance: once a message is acknowledged, the next goes to
whichever consumer polls first. The documentation says so explicitly, to avoid a false Kafka-style
expectation.

`HEAD` is the container's default strategy rather than PGMQ's `GREEDY`. `read_grouped` can return
several messages from one group in a batch, and row order within a batch is not guaranteed by the
SQL. `read_grouped_head` returns at most one message per group, so ordering holds however a batch
handler processes its contents. Every grouped variant has a `_with_poll` overload, so grouped
reads support long polling.

### Lifecycle

- A container is a `SmartLifecycle` with a phase just below the default, so it stops before most
  other beans. `stop(Runnable)` drains asynchronously, so a context's containers drain in
  parallel; a blocking `stop()` that arrives meanwhile waits for that drain.
- Polling loops are daemon threads - virtual threads on JDK 21+, which are always daemon. A running
  container therefore holds one idle non-daemon thread, so a consumer-only application stays up,
  as it would with other Spring messaging containers.
- `start()` fails when the queue or dead-letter queue does not exist, so a misspelt name is a
  startup error rather than a stream of poll errors. If the check cannot run, the container starts
  and keeps retrying.
- An outage logs one stack trace, then one line per further failed poll, then a recovery line.

### Observability without a Micrometer dependency

`PgmqClientListener` (producer side) and `ConsumerListener` (consumer side) let metrics, tracing
and application bookkeeping attach without `pgmq-core` depending on Micrometer. Several listeners
compose through `CompositeConsumerListener`, which isolates a failing delegate.

Containers are built by application code, which should not have to know about metrics. A
`BeanPostProcessor` in the metrics auto-configuration therefore attaches `PgmqMetrics` to every
container bean through `addListener`, which ignores a listener already attached - directly or
inside a composite - so a container wired by hand is not counted twice.

The health indicator queries the database on every check; capabilities alone are cached and would
keep reporting UP through an outage.

### Optional dependencies

Micrometer, `spring-boot-health`, both Jackson generations and SLF4J are `compileOnly` and
guarded. SLF4J is bound through `MethodHandle`s for MDC only; the core logs through
commons-logging, which Spring routes to the application's logging system. The starter itself
depends on `spring-boot-starter-json`, because PGMQ payloads are always JSON.

### Virtual threads on a Java 17 baseline

`Thread.ofVirtual()` is looked up reflectively once, with a fallback to daemon platform threads,
so the jar targets Java 17 without being a multi-release jar.

### Build

Multi-module Gradle, Groovy DSL, a version catalog and precompiled convention plugins in
`build-logic`. The Boot BOM version lives in `gradle.properties` so a build can override it with `-P`.

- Releases are built by JitPack from a git tag (`jitpack.yml`). The test fixtures - the shared
  Testcontainers bootstrap - are excluded from the published `pgmq-core`, so its POM carries no
  test dependencies.
- Dependency management uses Boot's BOM as a Gradle `platform()` on a resolve-only
  configuration that every classpath extends. It governs what the build resolves but is not
  published: the POM and Gradle metadata carry the resolved versions instead, so consumers - plain
  `pgmq-core` users included - do not inherit Boot's whole set of version constraints. The Boot
  plugin is not applied, because a library must not produce a `bootJar`.
- Spotless removes unused imports with the JavaParser-based engine: the google-java-format one
  needs `--add-exports` on modern JDKs and fails when Spotless tasks run in parallel.

---

## 4. Scope

| Not provided | Why |
|---|---|
| Topic routing (`bind_topic` / `send_topic`) | Exists from PGMQ 1.10.0, above the 1.5.0 minimum. Capability detection reports it. |
| Annotation-driven listeners | Containers are plain beans built with a builder. |
| Several named clients | `pgmq.datasource` selects one non-primary `DataSource`; more need hand-built `PgmqTemplate`s. |
| Spring Cloud Stream | PGMQ has no broker-side fan-out or partitions, so most of the binder model would have to be rejected or emulated. |

---

## 5. Known limitations

- **No fan-out.** A message goes to one queue. Broadcast needs PGMQ 1.10+ topic routing.
- **A batch shares one delay.** `sendMessages` gives each message its own headers, but PGMQ's
  `send_batch` takes one delay or delivery time for the whole batch.
- **Batch handlers are all-or-nothing.** A throw applies the failure handling to every message in
  the batch, including ones the handler had already processed.
- **Long polling holds a connection per consumer loop** for the whole window.
- **Dead-lettering without a transaction manager is not atomic**: a crash between the send to the
  dead-letter queue and the delete can duplicate or strand a message. The container warns at
  startup.
- **Archive tables grow without bound** under `ARCHIVE`. Prune them, or use a partitioned queue
  with a retention interval.
- **Queue gauges poll `pgmq.metrics()`**, which scans the queue table; the result is cached for one
  second per queue. They keep their last value while the database is unreachable - alert on
  `pgmq.poll.errors` or the health indicator for outages.

---

## 6. Testing

Integration tests run against **real PGMQ containers** through Testcontainers, at both ends of the
supported range: `-Dpgmq.image` selects the image, and the suite runs on 1.5.1 and 1.13.0.
Features above a version are skipped, not failed, on older images. Awaitility replaces sleeps.

Covered, among others: transactional sends that roll back; uncommitted sends invisible to other
connections; eight competing consumers draining with zero duplicates; redelivery with an
incremented read count; poison messages dead-lettered with failure headers; transactional
processing committing or rolling back with the acknowledgement; per-message payload conversion
failures; batch-wide leases; settlements that throw; graceful and parallel shutdown; grouped
ordering across consumers; health during an outage; concurrent startup; auto-configuration
back-off and property binding; and every sample end to end.

Two rules the suite follows:

- **Wiring is tested through the real wiring.** A test that supplies its own version of what
  production would construct - a hand-made `MeterRegistry`, say - cannot detect that the
  production path fails to construct it. Auto-configuration ordering is tested with Boot's own
  Micrometer auto-configuration for this reason.
- **A test must be able to fail.** Tests for failure handling are checked by reintroducing the
  defect they guard against and confirming they go red; a rollback test rolls back *after* the
  send, not before it.
