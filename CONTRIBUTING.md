# Contributing to pgmq-spring

Thanks for considering a contribution. This guide covers everything you need to
get from a clone to a merged pull request.

## Getting set up

You need:

| Tool | Version | Notes |
|---|---|---|
| JDK | **17 or newer** | The build compiles at the Java 17 baseline via a Gradle toolchain. If you do not have 17 installed, Gradle will download it (the foojay resolver is configured). |
| Docker | any recent version | **Required.** Integration tests run real Postgres + PGMQ containers via Testcontainers. There is no mock mode. |
| Gradle | none | Use the wrapper: `./gradlew`. It is pinned to 9.6.0 with a checksum. |

```bash
git clone https://github.com/igorsyrbu/pgmq-spring.git
cd pgmq-spring
./gradlew build
```

The first run pulls `ghcr.io/pgmq/pg17-pgmq:v1.13.0` (~400 MB) and takes a few
minutes. Later runs reuse it.

## Building and testing

```bash
./gradlew build                      # compile, check formatting, run everything
./gradlew test                       # tests only
./gradlew :pgmq-core:test            # one module
./gradlew test --tests '*DeadLetter*' # one test
```

### Testing against other PGMQ versions

The suite is version-parameterised. Run it against the minimum supported PGMQ
and the latest before a release:

```bash
./gradlew test -Dpgmq.image=ghcr.io/pgmq/pg17-pgmq:v1.5.1   # minimum supported
./gradlew test -Dpgmq.image=ghcr.io/pgmq/pg18-pgmq:v1.13.0  # latest, newest PG
```

If you touch anything that reads PGMQ's catalog or its composite types, run both.
The `pgmq.message_record` type genuinely differs between versions, and a change
that works on 1.13 can fail on 1.5.

### Testing against other Boot versions

```bash
./gradlew build -PspringBootVersion=4.1.1
./gradlew build -PtestJavaVersion=21
```

## Code style

Formatting is enforced by Spotless and checked as part of `build`.

```bash
./gradlew spotlessApply   # fix formatting
./gradlew spotlessCheck   # verify
```

Beyond the automated rules, match the surrounding code:

* **Spring conventions.** 4-space indent, 120-column lines, `this.` for field
  access, javadoc on public types and methods.
* **JSpecify nullability.** Every package has a `@NullMarked` `package-info.java`;
  mark nullable types with `@Nullable` from `org.jspecify.annotations`, placed
  immediately before the type (it is `TYPE_USE`).
* **Comments explain *why*, not *what*.** A comment earns its place by
  recording a non-obvious fact about PGMQ or Spring. Do not narrate the code,
  and do not record history - that is what commits are for.
* **No new hard dependencies** in `pgmq-core` without discussion. Micrometer,
  Jackson, SLF4J and Boot's health module are all deliberately optional.

## Tests are not optional

Every behavioural change needs a test. In particular:

* Anything touching SQL needs an integration test against a real container —
  PGMQ's overload resolution and type changes are exactly the kind of thing unit
  tests with mocks will happily lie about.
* Use **Awaitility**, never `Thread.sleep`, for asynchronous assertions.
* Name tests as sentences describing the behaviour
  (`redeliversAfterAHandlerCrashWithAnIncrementedReadCount`), not
  `testRedelivery`.
* A test must be able to fail. Check a failure-handling test by reintroducing
  the defect it guards against, and prefer Boot's real auto-configuration over
  hand-built beans when testing wiring.
* Tests must be stable. Run a timing-sensitive test several times before
  submitting:
  `./gradlew :pgmq-core:test --tests '*Container*' --rerun-tasks`

## Commits, branches and pull requests

* Branch from `main`, named `feat/short-description` or `fix/short-description`.
* Write [Conventional Commits](https://www.conventionalcommits.org/):
  `feat(core): ...`, `fix(autoconfigure): ...`, `docs: ...`, `test(core): ...`,
  `build: ...`, `refactor: ...`, `perf: ...`, `chore: ...`.
* The subject line is imperative and under 72 characters. The body explains
  *why*, and names anything surprising you discovered.
* One logical change per pull request. If you found an unrelated bug along the
  way, open a separate one.
* Fill in the pull request template. Say which PGMQ and Boot versions you tested
  against.
* `./gradlew build` must pass locally; there is no hosted CI.

## Developer Certificate of Origin

This project uses the [DCO](https://developercertificate.org/) rather than a CLA.
Sign off every commit:

```bash
git commit -s -m "fix(core): ..."
```

This appends `Signed-off-by: Your Name <you@example.com>`, which certifies that
you wrote the contribution or otherwise have the right to submit it under the
Apache 2.0 license. Set `user.name` and `user.email` in git first.

If you forget, `git rebase --signoff main` fixes the whole branch.

## Reporting bugs and proposing changes

* **Bugs**: open a bug report and include the pgmq-spring, Spring Boot, PGMQ and
  Postgres versions, plus the smallest configuration that reproduces it. A
  failing test is the best possible report.
* **Features**: open a feature request first and let a maintainer confirm the
  direction before writing code. This project deliberately keeps a small public
  surface and prefers an extension point over a configuration flag.
* **Questions**: use GitHub Discussions rather than a bug report.

## Good first issues

If you are looking for somewhere to start, these are self-contained and the
design work is already done — they are tracked as `good first issue`:

1. **`LISTEN/NOTIFY` consumer mode.** PGMQ 1.10 added `enable_notify_insert`;
   using it would remove polling entirely on new-enough servers.
2. **PGMQ topic routing.** `send_topic`/`bind_topic` exist from 1.10 and would
   give real fan-out. Capability detection already reports them.
3. **Archive retention helper.** A scheduled task that prunes archive tables.
