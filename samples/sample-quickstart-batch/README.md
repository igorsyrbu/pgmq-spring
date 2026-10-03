# sample-quickstart-batch

The quickstart in batches: **many messages per send, many messages per handler call**. Read
[`sample-quickstart`](../sample-quickstart) first; this one changes only what batching changes.

## What it shows

- **Batch sending.** `sendBatch` writes a whole list of messages in **one statement**, inside the
  same transaction as the business write. All of them commit, or none do.
- **Batch consumption.** A container with `batchHandler(...)` and `batchSize(50)` hands up to 50
  messages to one call, which writes them with one multi-row insert. The insert and the
  acknowledgement (one delete for every id) commit together.
- **Spreading the polls.** `pollJitter(100ms)` adds a random extra wait after every empty poll,
  so the two polling loops - and other instances deployed at the same time - do not hit the
  database in synchronised bursts.
- **The two rules of a batch handler.** A batch is retried and dead-lettered *as a whole*, so:
  - **be idempotent** - every message in a redelivered batch is seen again;
  - **throw only for problems a retry can fix.** An invalid reading is recorded as rejected
    rather than thrown, so its batch-mates still succeed. Throwing is for transient failures.

## Running it

```bash
# Skip this if the container is still running from sample-quickstart.
docker run -d --name pgmq -e POSTGRES_PASSWORD=postgres -p 5432:5432 \
    ghcr.io/pgmq/pg17-pgmq:v1.13.0


./gradlew :samples:sample-quickstart-batch:run
```

On startup it ingests three requests of 100 readings, each sent in one statement. The log shows
batches being handled. The second request contains an out-of-range reading, which ends up in
`rejected_readings`, and a reading from `flaky-sensor`, whose batch fails once and is redelivered.

```bash
docker exec -it pgmq psql -U postgres -c 'select count(*) from readings;'          # 299
docker exec -it pgmq psql -U postgres -c 'select * from rejected_readings;'         # 1
```

Pass `--args='--sample.demo.enabled=false'` to start it without the demo data.

## The code

| File | What to look at |
|---|---|
| [`ReadingService`](src/main/java/io/github/pgmqspring/samples/batch/ReadingService.java) | `@Transactional` - the ingestion row and `sendBatch` are one Postgres transaction |
| [`ReadingBatchHandler`](src/main/java/io/github/pgmqspring/samples/batch/ReadingBatchHandler.java) | One call per batch; idempotent multi-row insert; rejects instead of throwing |
| [`ReadingConsumerConfiguration`](src/main/java/io/github/pgmqspring/samples/batch/ReadingConsumerConfiguration.java) | `batchHandler(...)` and `batchSize(50)` - the only differences from the single-message container |
| [`application.yaml`](src/main/resources/application.yaml) | A datasource and two queue names |

## Variations

Per-message headers - for example a FIFO group key per device - need `sendMessages` instead of
`sendBatch`, which gives every message the same headers:

```java
pgmq.sendMessages(ReadingService.QUEUE, readings.stream()
        .map((r) -> OutboundMessage.of(r).group(r.device()))
        .toList());
```

## Tests

```bash
./gradlew :samples:sample-quickstart-batch:test
```

Four integration tests against a real PGMQ container: 200 readings sent in one statement and
consumed in batches; an invalid reading rejected without failing its batch; a transient failure
retrying the whole batch while storing every reading once; and a rollback after the send
discarding the whole batch.

## Next

- [`sample-quickstart`](../sample-quickstart) - one message at a time
