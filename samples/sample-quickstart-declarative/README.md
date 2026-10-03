# sample-quickstart-declarative

The quickstart with its consumers **declared in configuration**: handler beans, and a
`pgmq.consumers` entry for each. There is no `@Bean` for a listener container and no builder. Read
[`sample-quickstart`](../sample-quickstart) first; this one changes only where the consumers are
defined.

## What it shows

- **Consumers as configuration.** Each entry under `pgmq.consumers` names a handler bean and tunes
  its container. The starter builds it, registers it as a bean named `pgmqConsumer-<name>`, starts
  and stops it with the application, and attaches the Micrometer listener - exactly what a
  hand-built container bean gets.
- **Defaults and overrides.** Every entry inherits `pgmq.consumer.*` - here three attempts with
  exponential backoff - and overrides what it sets: `audit` allows five.
- **Little to spell out.** The queue defaults to the entry's name, and the payload type to the
  handler's type argument (`PgmqMessageHandler<InvoiceIssued>`), so `invoices` and `audit` need
  only a handler. The handler interface picks the style: `InvoiceHandler` takes one message at a
  time, `AuditBatchHandler` a batch.
- **Everything else as before.** `invoices` is transactional with a timeout, dead-letters an
  invoice for nothing on its first failure (a non-retryable exception), and retries others.

## Running it

```bash
# Skip this if the container is still running from another sample.
docker run -d --name pgmq -e POSTGRES_PASSWORD=postgres -p 5432:5432 \
    ghcr.io/pgmq/pg17-pgmq:v1.13.0


./gradlew :samples:sample-quickstart-declarative:run
```

On startup it issues three invoices. Two are emailed and audited; the third, for nothing, is
rejected and dead-lettered at once.

```bash
docker exec -it pgmq psql -U postgres -c 'select * from invoice_emails;'
docker exec -it pgmq psql -U postgres -c 'select * from audit_log;'
docker exec -it pgmq psql -U postgres -c 'select msg_id, read_ct, message from pgmq.q_invoices_dlq;'
```

Pass `--args='--sample.demo.enabled=false'` to start it without the demo invoices.

## The code

| File | What to look at |
|---|---|
| [`application.yaml`](src/main/resources/application.yaml) | `pgmq.consumer` defaults and the two `pgmq.consumers` entries - the whole consumer setup |
| [`InvoiceService`](src/main/java/io/github/pgmqspring/samples/declarative/InvoiceService.java) | `@Transactional` - the invoice row and both messages commit together |
| [`InvoiceHandler`](src/main/java/io/github/pgmqspring/samples/declarative/InvoiceHandler.java) | A `PgmqMessageHandler<InvoiceIssued>` bean; idempotent |
| [`AuditBatchHandler`](src/main/java/io/github/pgmqspring/samples/declarative/AuditBatchHandler.java) | A `PgmqBatchMessageHandler<AuditEntry>` bean |

## Tests

```bash
./gradlew :samples:sample-quickstart-declarative:test
```

Five integration tests against a real PGMQ container: the declared containers carry the configured
and inherited options; an invoice is emailed and audited; the audit trail is consumed in batches;
an invoice for nothing is dead-lettered on its first delivery; and declared containers report the
consumer meters.

## Next

- [`sample-quickstart`](../sample-quickstart) - the same consumers built in code
- [`sample-quickstart-batch`](../sample-quickstart-batch) - sending and consuming in batches
