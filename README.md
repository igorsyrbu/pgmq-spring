# pgmq-spring

**PGMQ for the Spring ecosystem** — a Spring Boot 4 starter for
[PGMQ](https://github.com/pgmq/pgmq), the Postgres-native message queue.

[![JitPack](https://jitpack.io/v/igorsyrbu/pgmq-spring.svg)](https://jitpack.io/#igorsyrbu/pgmq-spring)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Java](https://img.shields.io/badge/Java-17%2B-orange.svg)](https://docs.spring.io/spring-boot/system-requirements.html)

---

## Why this exists

If your application already has Postgres, you probably do not need a message broker to get a work
queue. PGMQ adds one as a set of SQL functions — and because it is *just Postgres*, a message can
be enqueued **in the same transaction** as the business data that produced it:

```java
@Transactional
public void placeOrder(Order order) {
    orderRepository.save(order);
    pgmq.send("orders", new OrderPlaced(order.id()));
    // If this method throws, the row and the message both disappear.
}
```

That is the transactional outbox pattern with no outbox table, no poller, and no CDC pipeline.
This project makes that a one-dependency experience in Spring Boot, and adds the operational
machinery a real consumer needs: retries, poison-message protection, dead-lettering, transactional
processing, metrics, health and graceful shutdown.

## Quickstart

**1. Start a Postgres with PGMQ.**

```bash
docker run -d --name pgmq -e POSTGRES_PASSWORD=postgres -p 5432:5432 ghcr.io/pgmq/pg17-pgmq:v1.13.0
```

The image includes PGMQ but doesn't enable it in any database. You don't have to enable it
yourself: on startup, the starter runs `CREATE EXTENSION` when PGMQ is missing from the
application's database (controlled by `pgmq.create-extension`, on by default). This requires the
`CREATE` privilege on that database, which the database owner has.

If your application's role can't create extensions - common in production - enable it once
yourself. For the container above, this waits until Postgres is up and then creates it:

```bash
docker exec pgmq sh -c 'until pg_isready -h localhost -U postgres -q; do sleep 1; done; psql -U postgres -c "CREATE EXTENSION IF NOT EXISTS pgmq;"'
```

For any other database, run `CREATE EXTENSION IF NOT EXISTS pgmq;` in it as a role with the
`CREATE` privilege, for example:

```bash
psql postgres://postgres:postgres@localhost:5432/postgres -c 'CREATE EXTENSION IF NOT EXISTS pgmq;'
```

**2. Add the starter.** Releases are served by [JitPack](https://jitpack.io), built from the git
tag. The group is `com.github.igorsyrbu.pgmq-spring`, each module is an artifact, and the version
is the tag.

```groovy
repositories {
    mavenCentral()
    maven { url 'https://jitpack.io' }
}

dependencies {
    implementation 'com.github.igorsyrbu.pgmq-spring:pgmq-spring-boot-starter:0.1.0'
    runtimeOnly 'org.postgresql:postgresql'
}
```

<details><summary>Maven</summary>

```xml
<repositories>
    <repository>
        <id>jitpack.io</id>
        <url>https://jitpack.io</url>
    </repository>
</repositories>

<dependency>
    <groupId>com.github.igorsyrbu.pgmq-spring</groupId>
    <artifactId>pgmq-spring-boot-starter</artifactId>
    <version>0.1.0</version>
</dependency>
```
</details>

The starter pulls in `pgmq-core` and `pgmq-spring-boot-autoconfigure` transitively. Depend on
`pgmq-core` alone to use the client and container without Spring Boot.

The starter brings Spring JDBC, transactions and Jackson, so nothing else is needed — it works
in a plain non-web application.

**3. Point it at the database and declare a queue.**

```yaml
spring:
  datasource:
    url: jdbc:postgresql://localhost:5432/postgres
    username: postgres
    password: postgres

pgmq:
  queues:              # created on startup if absent
    - name: orders
    - name: orders_dlq
```

**4. Send and consume.**

```java
@Service
public class OrderService {

    private final PgmqOperations pgmq;
    private final OrderRepository orders;

    @Transactional
    public void placeOrder(Order order) {
        orders.save(order);
        pgmq.send("orders", new OrderPlaced(order.id()));
    }
}

@Configuration
class OrderConsumerConfig {

    @Bean
    PgmqMessageListenerContainer<OrderPlaced> orderListener(
            PgmqOperations pgmq, PlatformTransactionManager tx, OrderHandler handler) {
        return PgmqMessageListenerContainer.builder(pgmq, "orders", OrderPlaced.class)
                .options(ConsumerOptions.builder()
                        .concurrency(4)
                        .transactional(true)
                        .maxAttempts(3)
                        .failureAction(FailureAction.DEAD_LETTER)
                        .deadLetterQueue("orders_dlq")
                        .build())
                .transactionManager(tx)
                .handler(handler::handle)
                .build();
    }
}
```

That is the whole setup. The container starts and stops with the application context, and while
it runs it keeps the application alive, so a consumer-only service needs no web server.

## Modules

| Artifact | Purpose |
|---|---|
| `pgmq-core` | Client and consumer runtime. Plain Spring JDBC/TX, no auto-configuration. Usable without Boot. |
| `pgmq-spring-boot-autoconfigure` | Boot auto-configuration, health indicator, Micrometer wiring. |
| `pgmq-spring-boot-starter` | What applications depend on. Pulls the above plus `spring-boot-starter-jdbc` and `spring-boot-starter-json`. |

Third-party starters may not be named `spring-boot-starter-*`, hence `pgmq-spring-boot-starter`.

## Delivery semantics — read this before going to production

PGMQ is a **competing-consumer queue** with **at-least-once** delivery. Three consequences:

**Handlers must be idempotent.** A consumer that finishes its work and then crashes before
acknowledging will see the same message again once the visibility timeout expires. There is no
configuration that makes this go away; it is inherent to at-least-once delivery. Deduplicate on a
business key, or make the work naturally repeatable.

**A second consumer is not a broadcast.** In Kafka or RabbitMQ, a second consumer group gets its
own copy of every message. PGMQ has no broker-side fan-out at the supported versions, so a second
consumer on the same queue **shares** the messages instead. For fan-out, send to one queue per
consumer.

**Concurrency is safe.** `pgmq.read()` selects rows `FOR UPDATE SKIP LOCKED`, so two consumers
never receive the same message at the same time. Scaling out is just running more instances.

### Transactional patterns

| Goal | How |
|---|---|
| Message and business write commit together | Call `send` inside an `@Transactional` method on the same `DataSource`. |
| Business write and acknowledgement commit together | `ConsumerOptions.transactional(true)` plus a `PlatformTransactionManager`. |
| Never lose a failed message | The default. On any unexpected failure the runtime touches nothing, and the visibility timeout redelivers. |
| Stop an endlessly failing message | `maxAttempts` plus `failureAction(DEAD_LETTER)`. |

`pgmq.datasource` can point the client at a different `DataSource` — but note that doing so
**gives up the outbox guarantee**, because the messages are then in a different transaction from
your data.

## Consumer runtime

```java
ConsumerOptions.builder()
    .concurrency(4)                          // parallel polling loops
    .batchSize(10)                           // messages per poll
    .visibilityTimeout(Duration.ofSeconds(30))
    .pollDelay(Duration.ofMillis(200))       // empty-queue backoff, doubles to maxPollDelay
    .longPoll(Duration.ofSeconds(5))         // optional: wait in the database instead
    .acknowledgeMode(AcknowledgeMode.DELETE) // or ARCHIVE, or MANUAL
    .failureAction(FailureAction.DEAD_LETTER)// terminal action once attempts are exhausted
    .maxAttempts(5)                          // compared against PGMQ's read_ct
    .retryDelay(Duration.ofSeconds(5))
    .deadLetterQueue("orders_dlq")
    .transactional(true)
    .extendLease(true)                       // keep the batch leased until each message is settled
    .shutdownTimeout(Duration.ofSeconds(30))
    .build();
```

**`failureAction` is the *terminal* action, not the per-failure action.** A failing message is
redelivered until its attempts run out, and only then is it dead-lettered or archived.

**Poison protection uses PGMQ's `read_ct`**, which the database increments on every read. Because
the count lives in the row rather than in the consumer, it survives restarts and is shared across
every instance of a scaled-out application.

**Long polling holds a connection.** `longPoll` waits inside Postgres, which removes empty-poll
churn but occupies one JDBC connection *and one Postgres backend* per concurrent consumer for the
whole window. Size the pool for at least `concurrency` connections plus whatever the application
itself needs, or the web tier will starve.

## Ordered, one-at-a-time processing per key

PGMQ's grouped reads (1.10+) give you something a plain competing-consumer queue cannot: for a
given key, **at most one message is in flight at a time across your whole fleet**, and that key's
messages are delivered in send order. It's the same model SQS FIFO queues use with their message
group id.

Tag messages with a key when sending, and read in grouped mode:

```java
pgmq.send("user_events", event, SendOptions.none().group(event.userId()));

// or many keys in one round trip
pgmq.sendMessages("user_events", events.stream()
        .map((e) -> OutboundMessage.of(e).group(e.userId()))
        .toList());
```

```java
PgmqMessageListenerContainer.builder(pgmq, "user_events", UserEvent.class)
        .options(ConsumerOptions.builder()
                .concurrency(8)        // 8 threads, still never two on the same user
                .groupOrdered(true)
                .build())
        .handler(handler::handle)
        .build();
```

Create the grouped-read index once per queue - `fifo-index: true` under `pgmq.queues[]`, or
`pgmq.createFifoIndex(queue)` - or every poll scans the queue table to rank groups.

**What this is not.** It is not partition assignment. No instance *owns* a key the way a Kafka
consumer owns a partition — once a message is acknowledged, the next one for that key goes to
whichever consumer polls first. The guarantee is "never concurrent, always in order", which is
what per-key ordering usually means in practice, but it is narrower than sticky ownership.

**Watch the visibility timeout.** If a handler outlives it, the lease lapses while work is still
in progress, the group unblocks, and another instance can pick up the same key — breaking the very
guarantee you enabled. Set the timeout above your worst-case handler duration, or use
`extendLease(true)`.

### Strategies

All three enforce the same guarantee and differ only in how one read's batch is spread across
groups. Behaviour below is what PGMQ 1.13.0 actually returns for 3 messages in group `A`, 2 in
`B`, reading 10:

| `groupStrategy` | Returns | Use when |
|---|---|---|
| `HEAD` (default here) | `A1, B1` | Safest — a batch can never hold two messages from one group, so ordering survives any batch handling |
| `GREEDY` | `A1, A2, A3, B1, B2` | Maximum throughput per round trip; a busy key can dominate a batch |
| `ROUND_ROBIN` | `A1, B1, A2, B2, A3` | One hot key must not starve the others |

Long polling works with all three. Conditional filters do not — PGMQ's grouped functions take no
conditional argument.

### Manual acknowledgement

```java
.acknowledgingHandler((message, ack) -> {
    if (!readyToProcess(message.payload())) {
        ack.retryLater(Duration.ofMinutes(5));       // relative backoff
        // or ack.retryAt(nextBillingWindow());      // absolute wall-clock instant
        return;
    }
    process(message.payload());
    ack.acknowledge();
})
```

A handler that calls nothing leaves the message in place, so it is redelivered — the safe default.

## Configuration reference

**[docs/CONFIGURATION.md](docs/CONFIGURATION.md)** documents every property, builder option, header,
MDC key, meter and health detail, with types, defaults and constraints. The most used:

```yaml
pgmq:
  queues:                         # created on startup if absent
    - name: orders
    - name: orders_dlq
  consumer:                       # defaults for the ConsumerOptions bean
    concurrency: 4
    visibility-timeout: 60s       # must cover the whole batch; rounded up to whole seconds
    max-attempts: 5               # compared against PGMQ's read_ct
    failure-action: dead-letter   # terminal action once attempts are exhausted
    dead-letter-queue: orders_dlq
    transactional: true           # handler writes and the ack commit together
  health:
    queues: [orders]
  metrics:
    queues: [orders, orders_dlq]
```

| Group | Properties |
|---|---|
| General | `pgmq.enabled`, `pgmq.datasource`, `pgmq.verify-on-startup`, `pgmq.minimum-version` |
| Queues | `pgmq.queues[].name`, `.kind`, `.partition-interval`, `.retention-interval`, `.fifo-index` |
| Consumer | `pgmq.consumer.concurrency`, `batch-size`, `visibility-timeout`, `poll-delay`, `max-poll-delay`, `long-poll`, `acknowledge-mode`, `failure-action`, `retry-delay`, `max-attempts`, `dead-letter-queue`, `transactional`, `extend-lease`, `shutdown-timeout`, `group-ordered`, `group-strategy` |
| Health | `pgmq.health.enabled`, `pgmq.health.queues`, `pgmq.health.max-queue-depth` |
| Metrics | `pgmq.metrics.enabled`, `pgmq.metrics.queues` |

`pgmq.consumer.*` only sets defaults: containers are built by your code, from the `ConsumerOptions`
bean — see [how consumer properties reach a container](docs/CONFIGURATION.md#how-consumer-properties-reach-a-container).
Invalid values fail at startup with a message naming the property.

## Observability

Micrometer is optional; the library degrades gracefully without it. Meters are tagged with `queue`:
counters for sent, received, acknowledged, failed, poison and dead-lettered messages and for failed
polls; timers for send and handler duration; and, for `pgmq.metrics.queues`, gauges for depth,
visible depth and oldest-message age. Consumer meters are attached to every listener container
bean automatically. The [full list](docs/CONFIGURATION.md#meters) gives tags and meanings.

During handling, `pgmq.queue`, `pgmq.messageId` and `pgmq.readCount` are in the MDC. The health
indicator queries the database on every check and reports the PGMQ version and, optionally,
per-queue depth.

## Compatibility and support policy

This library supports **the current Spring Boot minor plus the previous one**, compiles against the
oldest supported minor, and is tested against all of them.

| | Version |
|---|---|
| Spring Boot | **4.0.x** (compile baseline) and **4.1.x** |
| Spring Framework | 7.0.x (both Boot minors manage 7.0.9) |
| Java | 17 baseline; tested on 17, 21 and 25 |
| Jakarta EE | 11 |
| Gradle | 9.6 wrapper, Groovy DSL (Boot's plugin floor is 8.14) |
| PGMQ | **1.5.0** minimum; tested against 1.5.1 and 1.13.0 |
| Postgres | 14–18 (PGMQ publishes images for each) |

Spring Boot 3.x is **out of scope**: it is out of open-source support and on a different Spring
Framework generation. The library is versioned with semantic versioning, independently of Spring's
numbering.

### PGMQ version differences the client handles for you

PGMQ's SQL surface differs across the supported range:

- `pgmq.message_record` gained a `last_read_at` column in 1.10.0, so reads select `*` and map by
  column name rather than using a fixed column list.
- `pop()` gained a quantity argument; older versions pop one message per call.
- `set_vt()` gained array and `timestamptz` overloads.
- Topic routing and grouped (FIFO) reads exist only from 1.10+. Grouped reads are exposed and
  capability-gated: against an older PGMQ they raise a clear error naming the required version
  rather than failing obscurely.

Capabilities are probed from the catalog on startup rather than parsed from a version string,
because a SQL-only PGMQ installation has no `pg_extension` row to read a version from.

## Documentation

- [Configuration reference](docs/CONFIGURATION.md) — every property, option, header and meter
- [Design notes](docs/DESIGN.md) — the PGMQ contract, the decisions built on it, and the limits
- [Architecture overview](docs/ARCHITECTURE.md)
- [Contributing](CONTRIBUTING.md)

## Samples

Two runnable applications, each with integration tests against a real PGMQ container.

| Sample | What it shows |
|---|---|
| [`sample-quickstart`](samples/sample-quickstart) | The smallest useful app: a service that saves a row and sends a message in one transaction, and a listener container that consumes them. No HTTP layer. Start here. |
| [`sample-quickstart-batch`](samples/sample-quickstart-batch) | The same in batches: `sendBatch` sends a list in one statement inside the business transaction, and a batch handler consumes up to 50 per call with one multi-row insert — including how to keep one bad message from failing its whole batch. |

```bash
# one message at a time
./gradlew :samples:sample-quickstart:run

# batch sending and batch consumption
./gradlew :samples:sample-quickstart-batch:run

```

## Building

Requires JDK 17+ and Docker (for Testcontainers). The build is Gradle with the Groovy DSL, using a
version catalog and convention plugins in `build-logic`.

```bash
./gradlew build                                             # everything
./gradlew build -PspringBootVersion=4.1.1                   # another supported Boot minor
./gradlew test -Dpgmq.image=ghcr.io/pgmq/pg17-pgmq:v1.5.1   # the minimum supported PGMQ
```

## License

Apache License 2.0 — see [LICENSE](LICENSE) and [NOTICE](NOTICE).
