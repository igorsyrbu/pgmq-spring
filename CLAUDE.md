# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

pgmq-spring: a Spring Boot 4 starter for [PGMQ](https://github.com/pgmq/pgmq), the Postgres-native
message queue. Java 17 baseline, Spring Boot 4.0.x (compile baseline) and 4.1.x, Spring Framework 7,
Jackson 3 (Jackson 2 supported as a fallback), PGMQ 1.5.0–1.13.0. Gradle with the **Groovy** DSL.
Releases are built by JitPack from a git tag; there is no hosted CI.

## Commands

Docker must be running: integration tests start a real PGMQ container through Testcontainers
(there is no mock mode).

```bash
./gradlew build                                              # compile, Spotless check, all tests
./gradlew spotlessApply                                      # fix formatting (build fails on violations)
./gradlew :pgmq-core:test                                    # one module
./gradlew :pgmq-core:test --tests '*ContainerHardening*'     # one class
./gradlew :pgmq-core:test --tests '*PgmqTemplateHardening*aSubSecond*'   # one method
./gradlew test -Dpgmq.image=ghcr.io/pgmq/pg17-pgmq:v1.5.1    # oldest supported PGMQ (default image is v1.13.0)
./gradlew build -PspringBootVersion=4.1.1                    # another Boot minor
./gradlew test -PtestJavaVersion=21                          # run tests on a newer JDK
./gradlew :samples:sample-quickstart:run                     # needs Postgres+PGMQ on localhost:5432
./gradlew publishToMavenLocal -Pgroup=com.github.igorsyrbu -Pversion=0.2.0 -Dmaven.repo.local=/tmp/preview  # preview a JitPack release
```

Run anything that touches SQL or PGMQ's catalog on **both** v1.5.1 and v1.13.0: the SQL surface
differs between them, and features above a version are skipped (JUnit `Assumptions`) on older images.

After changing test sources, trust fresh results only: a test class that fails to compile leaves the
previous `build/test-results` XML in place. Delete `*/build/test-results` before reading them, or
check Gradle's exit code.

## Modules

- `pgmq-core` — everything that talks to PGMQ, with no Boot dependency: the client
  (`client/PgmqOperations`, `PgmqTemplate`), the consumer runtime (`consumer/PgmqMessageListenerContainer`),
  payload conversion (`convert/`), Micrometer instrumentation (`micrometer/`), capability probing
  (`PgmqCapabilities`) and `PgmqExtension`. Micrometer, both Jackson generations and SLF4J are
  `compileOnly` and guarded at runtime. `src/testFixtures` holds `PgmqContainerSupport`, the shared
  Testcontainers bootstrap used by every module's tests; it is excluded from the published artifact.
- `pgmq-spring-boot-autoconfigure` — `PgmqProperties` (all under `pgmq.*`), `PgmqAutoConfiguration`,
  `PgmqInitializer` (extension creation, version check, queue creation at startup), health and metrics
  auto-configuration. Every bean is `@ConditionalOnMissingBean`.
- `pgmq-spring-boot-starter` — no code, only dependencies.
- `samples/` — `sample-quickstart` (one message at a time), `sample-quickstart-declarative`
  (consumers declared under `pgmq.consumers`) and `sample-quickstart-batch` (batch send
  and batch handler). Samples are never published.
- `build-logic/` — precompiled convention plugins: `pgmq.java-conventions`, `pgmq.library-conventions`
  (publishing), `pgmq.sample-conventions`.

## Architecture that spans several files

**Client.** `PgmqTemplate` is a thin layer over `JdbcTemplate`, so every call joins the caller's Spring
transaction through `DataSourceUtils` — that is what makes `send` inside `@Transactional` an outbox with
no outbox table. Rules the SQL follows, each for a reason recorded in `docs/DESIGN.md`:
- Every parameter is cast (`?::text`, `?::jsonb`, …): PGMQ overloads `send`, `send_batch`, `set_vt` and
  the grouped reads so that untyped JDBC placeholders are ambiguous.
- Reads `select *` and map by column name: `pgmq.message_record` gains columns across versions.
- Rows are mapped as raw JSON and converted **after** the result set is read, per message
  (`convert()`), never inside the row mapper.
- Batch payloads travel as a JSON array of strings, each parsed on its own by the database, so one
  malformed document cannot turn into several messages.
- Durations become PGMQ's whole seconds by rounding **up** (`PgmqTemplate.seconds`).
- Version differences go through `PgmqCapabilities`, probed once from `pg_proc`/`pg_type` and cached;
  e.g. `setVisibleAt` falls back to a DB-computed integer delay where `set_vt(…, timestamptz)` is absent.
- Errors are translated by SQLSTATE first (`translate()`); `QueueNotFoundException` only when the
  missing table is the queue's own `pgmq.q_<name>`.

**Consumer runtime.** `PgmqMessageListenerContainer` is a `SmartLifecycle` bean built with a builder
from `ConsumerOptions` (Boot binds `pgmq.consumer.*` into a defaults bean; `toBuilder()` derives
per-container options), or declared under `pgmq.consumers.<name>`, which
`PgmqDeclaredConsumersRegistrar` turns into `pgmqConsumer-<name>` beans. `concurrency` poll loops (virtual threads on JDK 21+, via reflective
`VirtualThreads`) read a batch, convert each message, and dispatch to a single, acknowledging or batch
handler. Invariants to preserve when changing it:
- On any unexpected failure the container touches nothing and lets the visibility timeout redeliver.
- `failureAction` is **terminal**: applied only once `read_ct` reaches `maxAttempts`; earlier failures
  are retried with `retryDelay`. `read_ct > maxAttempts` on read means poison, and the handler is skipped.
- With `extendLease`, one `Lease` per polled batch refreshes every unsettled message; every settlement
  path calls `lease.settle(id)` **before** acting, and settle/refresh share a `ReentrantLock` (not
  `synchronized`: refresh holds it across JDBC, which would pin virtual threads on JDK 21–23).
- An `Acknowledgement` counts as settled only after its action succeeds; a handler that settles and then
  throws keeps its settlement outside a transaction.
- A running container holds one non-daemon keep-alive thread, so consumer-only apps stay up;
  `stop(Runnable)` drains asynchronously, and a blocking `stop()` joins a drain in progress.
- `start()` fails when the queue or dead-letter queue does not exist.

**Boot integration.** Health, metrics, gauges and the initializer key on `PgmqOperations`, not
`PgmqTemplate`, so a custom client keeps them. Metrics auto-configuration is ordered with `afterName`
against Boot's Micrometer auto-configuration, and a `BeanPostProcessor`
(`PgmqListenerContainerMetricsPostProcessor`) attaches `PgmqMetrics` to every container bean via
`addListener`, which de-duplicates. `PgmqInitializer` is excluded from lazy initialization.

**Publishing.** Boot's BOM is a `platform()` on a resolve-only configuration that every classpath
extends; it is not published. POMs and Gradle metadata carry the resolved versions (`versionMapping`),
so consumers do not inherit Boot's constraints. The Boot Gradle plugin is deliberately not applied.

## Testing conventions

- Integration tests share one container and pool from `PgmqContainerSupport`; isolate with
  `PgmqContainerSupport.uniqueQueueName(...)`, never by restarting the database. Spring test
  `DataSource` beans wrapping the shared pool must use `@Bean(destroyMethod = "")`.
- Use Awaitility, not sleeps, for assertions; keep timing margins wide (leases ≥ 3 s against handler
  steps of ~2 s) and prefer relative checks to wall-clock bounds.
- Wiring is tested through Boot's real auto-configuration (e.g. `MetricsAutoConfiguration`), not
  hand-built beans, and failure-handling tests are checked by reintroducing the defect and confirming
  they fail.

## Conventions

- Spring code style: 4-space indent, 120 columns, `this.` for fields, Javadoc on public API; every
  package has a `@NullMarked` `package-info.java`, with JSpecify `@Nullable` placed before the type.
- Comments record non-obvious facts about PGMQ or Spring, not history.
- Every configuration property needs Javadoc on its field in `PgmqProperties` (it becomes the IDE
  metadata) and an entry in `docs/CONFIGURATION.md`.
- No new hard dependencies in `pgmq-core`.
- Conventional Commits (`feat(core): …`, `fix(autoconfigure): …`, `docs: …`).

## Documentation

`docs/CONFIGURATION.md` (every property, option, header, MDC key, meter and health detail),
`docs/DESIGN.md` (the PGMQ contract, decisions, scope, known limitations), `docs/ARCHITECTURE.md`,
`RELEASING.md` (JitPack and local releases), `CONTRIBUTING.md`.
