# Configuration reference

Everything that configures pgmq-spring, in one place: Spring Boot properties, the programmatic
options behind them, message headers, logging context, meters and health details.

- [Spring Boot properties](#spring-boot-properties)
  - [General](#general-pgmq)
  - [Queues created on startup](#queues-created-on-startup-pgmqqueues)
  - [Consumer defaults](#consumer-defaults-pgmqconsumer)
  - [Health](#health-pgmqhealth)
  - [Metrics](#metrics-pgmqmetrics)
- [How consumer properties reach a container](#how-consumer-properties-reach-a-container)
- [Listener container builder](#listener-container-builder)
- [Read options](#read-options)
- [Send options](#send-options)
- [Batches with per-message headers](#batches-with-per-message-headers)
- [Headers](#headers)
- [Logging (MDC)](#logging-mdc)
- [Meters](#meters)
- [Health details](#health-details)
- [Beans you can replace](#beans-you-can-replace)
- [Durations and PGMQ's whole seconds](#durations-and-pgmqs-whole-seconds)
- [Sizing the connection pool](#sizing-the-connection-pool)

Durations accept Spring Boot's usual formats: `500ms`, `30s`, `5m`, or ISO-8601 (`PT30S`).
Enum values are case-insensitive (`dead-letter`, `DEAD_LETTER`).

---

## Spring Boot properties

All properties live under the single `pgmq` prefix and are present in the generated configuration
metadata, so an IDE completes and documents them. The one exception is the fields of
`pgmq.queues[]` list entries, which Boot's metadata format cannot describe; they are listed below.

### General (`pgmq.*`)

| Property | Type | Default | Description |
|---|---|---|---|
| `pgmq.enabled` | boolean | `true` | Master switch. `false` backs off the whole auto-configuration: no client, initializer, health indicator or metrics. |
| `pgmq.datasource` | string | *primary* | Name of the `DataSource` bean the client uses. Leave unset to use the primary one. A different data source keeps queues in another database, **at the cost of the transactional-outbox guarantee** between queue writes and entity writes. |
| `pgmq.create-extension` | boolean | `true` | Create the `pgmq` extension on startup when the database does not have it. Nothing is executed when PGMQ is already installed - as an extension or through its SQL-only script - so it costs nothing in production. Needs the `CREATE` privilege on the database; without it, startup fails with the database's reason. Several instances starting together are handled. Turn it off where extensions are managed by migrations or a DBA. |
| `pgmq.verify-on-startup` | boolean | `true` | Check during context startup that PGMQ is installed and at least `minimum-version`. Failure stops the application before it serves traffic. |
| `pgmq.minimum-version` | string | `1.5.0` | Lowest acceptable PGMQ version, as `major.minor.patch`. Startup fails on an older installation. Setting it below 1.5.0 does not make older versions work: 1.5.0 is the oldest the library supports. For a SQL-only installation (no `pg_extension` row), the check falls back to requiring message headers, which 1.5.0 introduced. |
| `pgmq.queues` | list | empty | Queues to create on startup if absent; see below. |

### Queues created on startup (`pgmq.queues[]`)

```yaml
pgmq:
  queues:
    - name: orders
    - name: orders_dlq
    - name: audit
      kind: unlogged
    - name: events
      kind: partitioned
      partition-interval: "1 day"
      retention-interval: "7 days"
    - name: user_events
      fifo-index: true        # for group-ordered consumers
```

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | *required* | Queue name. At most 47 characters, and must not contain `$`, `;`, `--` or a single quote. Names are case-insensitive: PGMQ lower-cases them. |
| `kind` | enum | `standard` | `standard` - a logged table. `unlogged` - faster, but **emptied by a crash or failover**; for data you can afford to lose. `partitioned` - partitioned by `pg_partman`, which must be installed. |
| `partition-interval` | string | `10000` | `partitioned` only. A number partitions by message id range; an interval such as `1 day` partitions by time. |
| `retention-interval` | string | `100000` | `partitioned` only. How many ids, or how much time, to keep before `pg_partman` drops old partitions. |
| `fifo-index` | boolean | `false` | Also create the index PGMQ's grouped reads use. Enable it for every queue consumed with `group-ordered=true`; without it each grouped poll scans the table. Requires PGMQ 1.10.0. |

Creation is idempotent, so the list is safe to apply on every start. An existing queue is left
alone - changing `kind` does not convert it. When several instances start together, a failed
creation is re-checked and ignored if the queue now exists.

### Consumer defaults (`pgmq.consumer.*`)

These bind to a `ConsumerOptions` bean. They are **defaults**: no container exists until your code
builds one - see [how they reach a container](#how-consumer-properties-reach-a-container).

| Property | Type | Default | Description |
|---|---|---|---|
| `concurrency` | int | `1` | Polling loops per container. Each holds a connection while it reads, and for the whole handler when `transactional`. Loops run on virtual threads on JDK 21+. At least 1. |
| `batch-size` | int | `10` | Maximum messages per poll. At least 1. |
| `visibility-timeout` | duration | `30s` | How long a read message stays hidden from other consumers. Must cover the worst-case handler time - and with `batch-size > 1`, **the whole batch**, because the lease starts at the read and the last message waits for the ones before it. Rounded up to whole seconds. Must be positive. |
| `poll-delay` | duration | `200ms` | Wait after an empty poll. Doubles on each further empty poll up to `max-poll-delay`, and resets on the first non-empty one. Also the re-check interval while paused. Must be positive. |
| `max-poll-delay` | duration | `5s` | Ceiling for the empty-queue backoff, and the wait after a failed poll (for example while the database is down). Must be at least `poll-delay`. |
| `long-poll` | duration | *unset* | When set, waits inside the database (`read_with_poll`) for up to this long instead of polling and backing off. Removes empty-poll traffic but **holds a connection per loop for the whole window**. At least `1s`. |
| `acknowledge-mode` | enum | `delete` | After the handler returns normally: `delete` the message; `archive` it into `pgmq.a_<queue>`; or `manual` - the handler decides through its `Acknowledgement`, and doing nothing leaves it for redelivery. `manual` needs a single-message acknowledging handler. |
| `failure-action` | enum | `redeliver` | The **terminal** action once a message has used up `max-attempts` - earlier failures are always retried. `dead-letter` moves it to `dead-letter-queue` with failure headers; `archive` archives it; `redeliver` keeps retrying and archives it once it becomes poison, because it has no terminal state of its own. |
| `retry-delay` | duration | `5s` | Backoff before a failed message with attempts left becomes visible again - after its first delivery, when `retry-multiplier` grows it. `0` leaves its current lease to expire instead. Rounded up to whole seconds. Must not be negative. |
| `retry-multiplier` | double | `1.0` | Grows the backoff with every failed attempt: after the n-th delivery a message waits `retry-delay × retry-multiplier^(n - 1)`, capped at `max-retry-delay` - with `retry-delay: 2s` and `2.0`, that is 2s, 4s, 8s... The attempt number is PGMQ's `read_ct`, so the backoff survives restarts and is the same on every instance. `1.0` keeps the delay fixed. At least `1.0`. |
| `max-retry-delay` | duration | *unset* | Upper bound for a backoff grown by `retry-multiplier`. Unset leaves it uncapped; beyond PGMQ's largest visibility timeout (`Integer.MAX_VALUE` seconds) it saturates. Must not be shorter than `retry-delay`. |
| `max-attempts` | int | `5` | Deliveries before a message is poison. Compared against PGMQ's `read_ct`, which survives restarts and is shared by all instances. A message read with `read_ct > max-attempts` gets the terminal action without the handler running. At least 1. |
| `dead-letter-queue` | string | *unset* | Target for `failure-action=dead-letter` (required then). Must exist when the container starts, and must not be the consumed queue. |
| `transactional` | boolean | `false` | Run the handler and its acknowledgement in one transaction, so business writes and the ack commit or roll back together. Needs a `PlatformTransactionManager` on the same `DataSource`. |
| `extend-lease` | boolean | `false` | Keep extending the lease of every message in a polled batch until it is settled - including messages waiting their turn and those of a batch handler. Refreshed every third of `visibility-timeout`, but never more often than every 200 ms. |
| `batch-acknowledgements` | boolean | `false` | Acknowledge the messages of a polled batch with one `delete` (or `archive`) statement when the batch is done, instead of one statement per message: a batch of 10 costs 2 round trips instead of 11. Applies to single-message handlers; a batch handler is always acknowledged with one statement. Until the flush each message stays leased - with `extend-lease`, its lease keeps being refreshed - so a crash or a failed flush redelivers it. Handler-initiated settlements (`Acknowledgement` calls) are still sent at once. Cannot be combined with `transactional` or `acknowledge-mode=manual`. |
| `ack-batch-size` | int | *unset* | With `batch-acknowledgements`, flush as soon as this many acknowledgements are pending instead of once per polled batch. Unset flushes once per batch; a value of at least `batch-size` has no effect. At least 1 when set. |
| `shutdown-timeout` | duration | `30s` | How long stopping waits for running handlers before interrupting them; their messages are redelivered once the lease lapses. Containers drain in parallel. Keep it below `spring.lifecycle.timeout-per-shutdown-phase` (30s by default in Boot). Must not be negative. |
| `group-ordered` | boolean | `false` | Read with PGMQ's grouped reads: per `x-pgmq-group` value, at most one message is in flight across all consumers and messages arrive in send order. Messages without the header share one implicit group. Requires PGMQ 1.10.0; pair with `pgmq.queues[].fifo-index`. |
| `group-strategy` | enum | `head` | How a grouped read fills a batch. `head` - at most one message per group, so a batch never holds two from one key (the only strategy where per-key order survives any handler). `greedy` - fills from the oldest group first. `round-robin` - interleaves groups so one busy key cannot monopolise a batch. Ignored unless `group-ordered`. |

Invalid combinations fail at startup with a message naming the property, for example
`failure-action=dead-letter` without `dead-letter-queue`, `poll-delay: 0`, `long-poll: 500ms`, or
`batch-acknowledgements` together with `transactional`.

### Health (`pgmq.health.*`)

| Property | Type | Default | Description |
|---|---|---|---|
| `pgmq.health.enabled` | boolean | `true` | Contribute the `pgmq` health indicator. Boot's `management.health.pgmq.enabled` switches it off too. Requires `spring-boot-health` on the classpath. |
| `pgmq.health.queues` | list | empty | Queues whose depth the indicator reports. Empty means only reachability, presence and version are checked - which keeps a load balancer's probe cheap. A listed queue that does not exist is reported as a detail and does not turn the indicator DOWN. |
| `pgmq.health.max-queue-depth` | long | `-1` | Report DOWN when any monitored queue holds more than this many messages. Negative disables the threshold. Think twice before using it for a liveness probe: a backlog is rarely fixed by a restart. |

### Metrics (`pgmq.metrics.*`)

| Property | Type | Default | Description |
|---|---|---|---|
| `pgmq.metrics.enabled` | boolean | `true` | Record Micrometer meters. Active only when a `MeterRegistry` exists (for example with `spring-boot-starter-actuator`). |
| `pgmq.metrics.queues` | list | empty | Queues published as depth and age gauges. Each is refreshed from `pgmq.metrics()` at most once a second, however often it is scraped. |

The percentiles of the library's timers are not published by default; enable them the usual Boot
way, for example `management.metrics.distribution.percentiles.pgmq.processing.duration=0.5,0.95,0.99`.

---

## How consumer properties reach a container

The starter does not create containers - which queue, which payload type and which handler are
application decisions. `pgmq.consumer.*` becomes a `ConsumerOptions` bean; pass it to the builder,
and derive per-container variants with `toBuilder()`:

```java
@Bean
PgmqMessageListenerContainer<OrderPlaced> orders(PgmqOperations pgmq, ConsumerOptions defaults,
        PlatformTransactionManager tx, OrderHandler handler) {
    return PgmqMessageListenerContainer.builder(pgmq, "orders", OrderPlaced.class)
            .options(defaults.toBuilder()
                    .concurrency(8)
                    .failureAction(FailureAction.DEAD_LETTER)
                    .deadLetterQueue("orders_dlq")
                    .build())
            .transactionManager(tx)
            .handler(handler::handle)
            .build();
}
```

Declared as a bean, the container starts and stops with the context, and - when metrics are
enabled - Spring Boot attaches the Micrometer listener to it, so it reports the consumer meters
without any wiring.

## Listener container builder

`PgmqMessageListenerContainer.builder(pgmq, queue, payloadType)`:

| Method | Description |
|---|---|
| `options(ConsumerOptions)` | Every setting in the consumer table above. Defaults to `ConsumerOptions.defaults()`. |
| `handler(PgmqMessageHandler<T>)` | One message at a time; the container acknowledges per `acknowledgeMode`. |
| `acknowledgingHandler(PgmqAcknowledgingMessageHandler<T>)` | One message at a time, with an `Acknowledgement` to settle it yourself: `acknowledge()`, `archive()`, `retryLater(Duration)`, `retryAt(Instant)`, `deadLetter(reason)`. Call at most one. |
| `batchHandler(PgmqBatchMessageHandler<T>)` | The whole batch at once. All or nothing: a throw applies the failure handling to every message in it. Not combinable with `MANUAL`. |
| `transactionManager(PlatformTransactionManager)` | Required for `transactional`, and what makes dead-lettering atomic (send to the dead-letter queue and delete from the source in one transaction). Without it the container warns at startup. |
| `listener(ConsumerListener)` | Your own callbacks, for example to react to dead-lettered messages. Combine several with `CompositeConsumerListener`. The Micrometer listener does not need to be passed: Boot attaches it to every container bean, and skips a container that already has it. `container.addListener(...)` adds one after building. |
| `verifyQueuesOnStart(boolean)` | Default `true`: `start()` fails if the queue or its dead-letter queue does not exist. If the check cannot run (database unreachable) the container starts anyway and keeps retrying. Disable only when the queue is created after the container starts. |

On the built container: `setPhase(int)` (default `DEFAULT_PHASE - 100`, so it stops before most
other beans), `setAutoStartup(boolean)` (default `true`), `setBeanName(String)` (thread names and
logs), `pause()` / `resume()` / `isPaused()`, `start()` / `stop()` / `close()`.

A running container keeps the JVM alive with one idle non-daemon thread, released when it stops,
so a worker application with no web server does not exit straight after startup. Its polling
loops themselves are daemon threads - always so on JDK 21+, where they are virtual.

A payload that cannot be converted to `payloadType` fails **that message only**: it counts towards
`max-attempts` and then gets the terminal action, like a handler that threw. Its batch-mates are
processed normally.

## Read options

`ReadOptions`, for direct `PgmqOperations.read(...)` / `readGrouped(...)` calls:

| Method | Default | Description |
|---|---|---|
| `defaults()` | | One message, 30s visibility timeout. |
| `batch(n)` / `batchSize(n)` | `1` | Messages per read. |
| `visibilityTimeout(Duration)` | `30s` | Lease length; rounded up to whole seconds. |
| `longPoll(Duration)` | *off* | Use `read_with_poll`, waiting up to this long; whole seconds. |
| `longPoll(Duration, Duration)` | `100ms` | Same, re-checking at the given interval. |
| `conditional(Map)` | *none* | Only messages whose payload contains this JSON fragment (`@>`). Not supported by grouped reads. |
| `groupStrategy(GroupReadStrategy)` | `GREEDY` | For `readGrouped` only. Note the container defaults to `HEAD` instead. |

`read(queue, options, Type.class)` throws `PayloadConversionException` if any message cannot be
converted - after PGMQ has already leased the batch. For untrusted producers read as `String`
(`read(queue, options)`) and convert each message with `pgmq.convert(message, Type.class)`.

## Send options

`SendOptions`, immutable; every method returns a copy:

| Method | Description |
|---|---|
| `none()` | No headers, immediate delivery. |
| `headers(Map)` / `withHeaders(Map)` | Replace all headers. Values must be JSON-serialisable. |
| `withHeader(name, value)` | Add one header. |
| `group(key)` | Set `x-pgmq-group`, placing the message in a FIFO group for grouped reads. |
| `delayed(Duration)` / `delay(Duration)` | Invisible for this long; whole seconds, rounded up. Clears `deliverAt`. |
| `deliverAt(Instant)` | Invisible until this instant, independent of the JVM time zone. Clears `delay`. |

`sendBatch` and `sendRawBatch` apply one `SendOptions` - headers included - to every message.
For headers that differ per message, use `sendMessages`.

## Batches with per-message headers

`sendMessages(queue, List<OutboundMessage>[, SendOptions])` sends a batch in one statement where
each message carries its own headers - so one batch can feed several FIFO groups:

```java
pgmq.sendMessages("user_events", events.stream()
        .map((e) -> OutboundMessage.of(e).group(e.userId()).withHeader("trace", traceId))
        .toList(),
        SendOptions.headers(Map.of("source", "importer")));
```

| `OutboundMessage` method | Description |
|---|---|
| `of(payload)` | A payload for the configured converter to serialize. |
| `ofJson(json)` | An already-serialized JSON document, stored verbatim (the batch form of `sendRaw`). It must be exactly one JSON value: text such as `{"a":1},{"b":2}` fails the whole batch rather than becoming two messages. |
| `withHeader(name, value)` / `withHeaders(Map)` | Add one header / replace all. Values must be non-null and JSON-serializable. |
| `group(key)` | Set `x-pgmq-group` for this message. |

- Headers in the `SendOptions` are **defaults** for every message; a message's own header of the
  same name wins.
- The delay or delivery time in `SendOptions` applies to the **whole batch**: PGMQ's `send_batch`
  takes a single one. Send separately when messages need different delays.
- Ids come back in the order of the list, and messages are enqueued in that order.
- A message with no headers at all is stored with `NULL` headers, the same as a single send.

## Headers

| Header | Written by | Meaning |
|---|---|---|
| `x-pgmq-group` | `SendOptions.group(key)` | FIFO group key read by PGMQ's grouped reads. Messages without it form the group `_default_fifo_group`. |
| `x-pgmq-original-queue` | dead-lettering | Queue the message failed on. |
| `x-pgmq-original-msg-id` | dead-lettering | Its id in that queue. |
| `x-pgmq-original-enqueued-at` | dead-lettering | When it was first sent. |
| `x-pgmq-read-count` | dead-lettering | Deliveries at the time it was dead-lettered. |
| `x-pgmq-failed-at` | dead-lettering | When it was dead-lettered. |
| `x-pgmq-failure-reason` | dead-lettering | Why: attempts exhausted with the exception summary, poison, or the handler's own reason. |
| `x-pgmq-exception-type` | dead-lettering | Fully qualified class of the root exception, when there was one. |
| `x-pgmq-exception-message` | dead-lettering | Its message, truncated to 2000 characters. |

Dead-lettering keeps the original headers and payload and adds these. Headers that are not a JSON
object (possible when another client wrote the message) are ignored with a warning.

## Logging (MDC)

While a message is handled - by the handler and by the container's own log lines about it:

| Key | Value |
|---|---|
| `pgmq.queue` | Queue name |
| `pgmq.messageId` | Message id |
| `pgmq.readCount` | Delivery number, starting at 1 |

Used only when SLF4J is on the classpath. Previous values are restored afterwards.

## Meters

All tagged `queue`. Consumer meters are recorded for every listener container bean: Boot attaches
the `PgmqMetrics` listener automatically while `pgmq.metrics.enabled` is `true`. A container built
outside the application context needs `.listener(pgmqMetrics)` instead.

| Meter | Type | Extra tags | Meaning |
|---|---|---|---|
| `pgmq.messages.sent` | counter | | Messages written. |
| `pgmq.send.duration` | timer | | One send statement; a batch send is one statement. |
| `pgmq.messages.received` | counter | | Messages read by a container. |
| `pgmq.messages.acknowledged` | counter | | Handler returned normally. |
| `pgmq.messages.failed` | counter | `exception` | Handler threw, or the payload could not be converted. |
| `pgmq.processing.duration` | timer | `outcome` = `success`/`failure` | Time in the handler. |
| `pgmq.messages.poison` | counter | | Read with `read_ct > max-attempts`. |
| `pgmq.messages.dead.lettered` | counter | `dead.letter.queue` | Moved to a dead-letter queue. |
| `pgmq.poll.errors` | counter | `exception` | Polls that failed, typically a database outage. |
| `pgmq.queue.length` | gauge | | Messages in the queue, visible or not (`pgmq.metrics.queues` only). |
| `pgmq.queue.visible.length` | gauge | | Messages available to read now. |
| `pgmq.queue.oldest.message.age` | gauge (seconds) | | Age of the oldest message. |

## Health details

The `pgmq` indicator reads PGMQ's queue list on every check, so it reports DOWN during an outage
or when PGMQ is not installed. It works with any `PgmqOperations` bean, not only `PgmqTemplate`.

| Detail | When |
|---|---|
| `version` | Always; `unknown (sql-only install)` without a `pg_extension` row. |
| `installedAsExtension` | Always. |
| `queues` | Always: how many queues PGMQ has. |
| `queue.<name>.length`, `.visibleLength`, `.oldestMessageAgeSeconds` | For each of `pgmq.health.queues`. |
| `queue.<name>.error` | When a monitored queue cannot be read, typically because it does not exist. The status stays UP: PGMQ itself is healthy, and restarting the instance would not fix the configuration. |
| `queue.<name>.exceededMaxDepth` | With status DOWN, when the queue is deeper than `max-queue-depth`. |
| `error` | With status DOWN, when the database is unreachable or PGMQ is not installed, alongside the exception. |

## Beans you can replace

Every bean is `@ConditionalOnMissingBean`; declare your own to replace it.

| Bean | Default |
|---|---|
| `PayloadConverter` | The application's Jackson 3 mapper bean; else its Jackson 2 `ObjectMapper` bean; else a default Jackson 3 mapper, or Jackson 2 when only that is on the classpath. |
| `PgmqOperations` | `PgmqTemplate` on the selected `DataSource`. Replacing it keeps health, the initializer, gauges and consumer meters; only the send meters need a `PgmqTemplate`. |
| `ConsumerOptions` | Bound from `pgmq.consumer.*`. |
| `PgmqInitializer` | Startup verification and queue creation. |
| `PgmqHealthIndicator` (name `pgmqHealthIndicator`) | See above. |
| `PgmqMetrics`, `PgmqQueueGauges` | Micrometer meters. |

## Durations and PGMQ's whole seconds

PGMQ takes visibility timeouts, delays and long-poll windows in whole seconds. The library rounds
**up** - `500ms` becomes 1 second, never 0 - because truncation would make a message visible to
every other consumer the instant it was read. Values too large for an `int` saturate. The
container's poll delays are handled in the JVM and keep millisecond precision.

## Sizing the connection pool

Budget, per application instance:

- one connection per consumer loop (`concurrency`) of every container - held for the whole
  handler when `transactional`, and for the whole window when `long-poll` is set;
- plus one while a lease is being refreshed (`extend-lease`), per container;
- plus whatever the application itself uses.

Too small a pool shows up as rising `hikaricp.connections.pending` and connection-acquire time,
long before it shows up as errors.
