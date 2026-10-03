# sample-quickstart

The smallest useful pgmq-spring application: **one service that sends, one container that
consumes**. No HTTP layer, no chunking, no bookkeeping. Start here.

## What it shows

- Sending a message **inside the same transaction** as a business write, so the two commit or roll
  back together — the transactional outbox with no outbox table.
- Consuming with `PgmqMessageListenerContainer`, declared as an ordinary bean.
- Transactional processing bounded by a transaction timeout, retries with exponential backoff,
  and dead-lettering after three failed attempts - or at once, for an exception that is not
  retryable.
- A second, non-transactional consumer configured entirely from `pgmq.consumer.*`, which
  acknowledges each polled batch with one statement (`batch-acknowledgements`) and wakes up on
  PGMQ's insert notifications instead of polling an idle queue (`wake-up: notify`, with
  `notify-on-insert` on its queue; PGMQ 1.10+).
- A default `source` header on every message sent, from `pgmq.producer.default-headers`.
- The health indicator and Micrometer metrics the starter contributes for free, with queue gauges
  refreshed every 10 seconds - and every 2 for `orders`, through `pgmq.metrics.refresh-intervals`.

## Running it

```bash
docker run -d --name pgmq -e POSTGRES_PASSWORD=postgres -p 5432:5432 \
    ghcr.io/pgmq/pg17-pgmq:v1.13.0


./gradlew :samples:sample-quickstart:run
```

The starter creates the `pgmq` extension in the `postgres` database on first start, so there is no
manual `CREATE EXTENSION` step.

On startup it places three orders. Two succeed, and each confirmation publishes an
`OrderConfirmed` event that the notification consumer records; the third has a negative total,
which the handler rejects with a non-retryable exception, so it is dead-lettered at once. You should see the
confirmations, the notifications and then the dead-letter in the log.

```bash
docker exec -it pgmq psql -U postgres -c 'select * from order_confirmations;'
docker exec -it pgmq psql -U postgres -c 'select * from order_notifications;'
docker exec -it pgmq psql -U postgres -c 'select msg_id, read_ct, message from pgmq.q_orders_dlq;'
```

Pass `--args='--sample.demo.enabled=false'` to start it without the demo orders.

## The code

| File | What to look at |
|---|---|
| [`OrderService`](src/main/java/io/github/pgmqspring/samples/quickstart/OrderService.java) | `@Transactional` — the insert and the `send` are one Postgres transaction |
| [`OrderHandler`](src/main/java/io/github/pgmqspring/samples/quickstart/OrderHandler.java) | An idempotent handler. At-least-once delivery makes this mandatory, not optional. It publishes `OrderConfirmed` in its own transaction |
| [`NotificationHandler`](src/main/java/io/github/pgmqspring/samples/quickstart/NotificationHandler.java) | A handler whose single idempotent write needs no transaction, so its acknowledgements can be batched |
| [`OrderConsumerConfiguration`](src/main/java/io/github/pgmqspring/samples/quickstart/OrderConsumerConfiguration.java) | Both listener containers: one with options in code, one from `pgmq.consumer.*` |
| [`application.yaml`](src/main/resources/application.yaml) | The whole configuration: a datasource, the queues and the consumer defaults |

## Tests

```bash
./gradlew :samples:sample-quickstart:test
```

Seven integration tests against a real PGMQ container: send/consume, bulk consumption, the
batch-acknowledging notification consumer, a rollback after the send leaving neither row nor
message, dead-lettering, pause/resume, and the auto-configured health indicator and metrics.

## Next

- [`sample-quickstart-batch`](../sample-quickstart-batch) — the same, sending and consuming in batches
- [`sample-quickstart-declarative`](../sample-quickstart-declarative) — the same consumers declared in
  configuration, with no container code
