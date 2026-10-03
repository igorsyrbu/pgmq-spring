/*
 * Copyright 2026 the pgmq-spring authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.github.pgmqspring.core.client;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.jspecify.annotations.Nullable;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.Assert;

import io.github.pgmqspring.core.InvalidQueueNameException;
import io.github.pgmqspring.core.PgmqCapabilities;
import io.github.pgmqspring.core.PgmqException;
import io.github.pgmqspring.core.PgmqMessage;
import io.github.pgmqspring.core.PgmqNotInstalledException;
import io.github.pgmqspring.core.PgmqVersion;
import io.github.pgmqspring.core.QueueInfo;
import io.github.pgmqspring.core.QueueKind;
import io.github.pgmqspring.core.QueueMetrics;
import io.github.pgmqspring.core.QueueNames;
import io.github.pgmqspring.core.QueueNotFoundException;
import io.github.pgmqspring.core.convert.PayloadConversionException;
import io.github.pgmqspring.core.convert.PayloadConverter;

/**
 * The default {@link PgmqOperations} implementation, built on {@link JdbcTemplate}.
 *
 * <p>Using {@code JdbcTemplate} is what gives the client its transaction semantics for free: it
 * obtains connections through {@code DataSourceUtils}, which returns the connection bound to the
 * current Spring transaction when there is one.
 *
 * <h2>Why every statement casts its parameters</h2>
 *
 * <p>PGMQ overloads {@code send} six ways, differing only in whether the third argument is
 * {@code jsonb} (headers), {@code integer} (delay seconds) or {@code timestamptz} (delivery time).
 * A JDBC call that binds an untyped parameter - which is what {@code ?} is before Postgres has
 * inferred a type - cannot be resolved:
 *
 * <pre>
 * ERROR: function pgmq.send(unknown, unknown, unknown) is not unique
 * HINT:  Could not choose a best candidate function.
 * </pre>
 *
 * <p>Every statement below therefore targets exactly one overload with explicit casts on every
 * parameter, which removes the ambiguity regardless of PGMQ version or driver behaviour.
 *
 * <h2>Why reads use {@code select *}</h2>
 *
 * <p>The {@code pgmq.message_record} composite type is not stable across versions: 1.5 has six
 * columns, 1.10 added {@code last_read_at}. Selecting a fixed column list would fail against one
 * version or the other, so reads select everything and map by column <em>name</em>, tolerating
 * absent columns.
 */
public class PgmqTemplate implements PgmqOperations {

    private static final org.apache.commons.logging.Log LOGGER =
            org.apache.commons.logging.LogFactory.getLog(PgmqTemplate.class);

    // Sends always target the 4-argument overload so exactly one candidate matches.
    private static final String SQL_SEND_DELAY_SECONDS =
            "select * from pgmq.send(?::text, ?::jsonb, ?::jsonb, ?::integer)";

    private static final String SQL_SEND_DELIVER_AT =
            "select * from pgmq.send(?::text, ?::jsonb, ?::jsonb, ?::timestamptz)";

    // Batch arrays are passed as a single JSON array parameter and expanded server-side, which
    // avoids java.sql.Array plumbing and keeps the client free of driver-specific code.
    //
    // Payloads travel as an array of JSON *strings*, each parsed on its own by the ::jsonb cast.
    // Splicing the documents' text into one array literal instead would let a malformed document
    // such as '{"a":1},{"b":2}' silently become two messages - misaligning the ids returned and
    // any per-message headers - where a single send() would have rejected it.
    private static final String PAYLOAD_ARRAY =
            "(select array_agg((value #>> '{}')::jsonb order by ord) "
                    + "from jsonb_array_elements(?::jsonb) with ordinality as t(value, ord))";

    // Headers use the same expansion, except that a JSON null element becomes SQL NULL: a message
    // without headers in a batch is then stored exactly like one sent alone without headers.
    private static final String JSONB_HEADER_ARRAY =
            "(select array_agg(nullif(value, 'null'::jsonb) order by ord) "
                    + "from jsonb_array_elements(?::jsonb) with ordinality as t(value, ord))";

    private static final String SQL_SEND_BATCH_DELAY_SECONDS =
            "select * from pgmq.send_batch(?::text, " + PAYLOAD_ARRAY + ", " + JSONB_HEADER_ARRAY + ", ?::integer)";

    private static final String SQL_SEND_BATCH_DELIVER_AT =
            "select * from pgmq.send_batch(?::text, " + PAYLOAD_ARRAY + ", " + JSONB_HEADER_ARRAY + ", ?::timestamptz)";

    private static final String SQL_READ = "select * from pgmq.read(?::text, ?::integer, ?::integer, ?::jsonb)";

    private static final String SQL_READ_WITH_POLL =
            "select * from pgmq.read_with_poll(?::text, ?::integer, ?::integer, ?::integer, ?::integer, ?::jsonb)";

    // One statement per grouped-read strategy, each with a long-polling counterpart. Every
    // grouped variant has a _with_poll overload (verified against PGMQ 1.13.0's catalog).
    private static final String SQL_READ_GROUPED =
            "select * from pgmq.read_grouped(?::text, ?::integer, ?::integer)";

    private static final String SQL_READ_GROUPED_POLL =
            "select * from pgmq.read_grouped_with_poll(?::text, ?::integer, ?::integer, ?::integer, ?::integer)";

    private static final String SQL_READ_GROUPED_HEAD =
            "select * from pgmq.read_grouped_head(?::text, ?::integer, ?::integer)";

    private static final String SQL_READ_GROUPED_HEAD_POLL =
            "select * from pgmq.read_grouped_head_with_poll(?::text, ?::integer, ?::integer, ?::integer, ?::integer)";

    private static final String SQL_READ_GROUPED_RR =
            "select * from pgmq.read_grouped_rr(?::text, ?::integer, ?::integer)";

    private static final String SQL_READ_GROUPED_RR_POLL =
            "select * from pgmq.read_grouped_rr_with_poll(?::text, ?::integer, ?::integer, ?::integer, ?::integer)";

    private static final String SQL_CREATE_FIFO_INDEX = "select pgmq.create_fifo_index(?::text)";

    // Queue names are passed lower-cased: enable_notify_insert stores the name as given, while its
    // trigger looks the throttle up by the lower-cased table name, so a mixed-case name would
    // never notify.
    private static final String SQL_ENABLE_NOTIFY_INSERT = "select pgmq.enable_notify_insert(?::text)";

    private static final String SQL_ENABLE_NOTIFY_INSERT_THROTTLED =
            "select pgmq.enable_notify_insert(?::text, ?::integer)";

    private static final String SQL_DISABLE_NOTIFY_INSERT = "select pgmq.disable_notify_insert(?::text)";

    // The trigger name is fixed by PGMQ (verified on 1.10.0 and 1.13.0); the throttle row alone
    // can outlive a trigger dropped by hand.
    private static final String SQL_NOTIFY_INSERT_THROTTLE =
            "select t.throttle_interval_ms from pgmq.notify_insert_throttle t where t.queue_name = ?::text "
                    + "and exists (select 1 from pg_trigger g join pg_class c on c.oid = g.tgrelid "
                    + "join pg_namespace n on n.oid = c.relnamespace where n.nspname = 'pgmq' "
                    + "and c.relname = ?::text and g.tgname = 'trigger_notify_queue_insert_listeners')";

    private static final String SQL_SET_VT_ONE_AT =
            "select msg_id from pgmq.set_vt(?::text, ?::bigint, ?::timestamptz)";

    // For PGMQ without the timestamptz overload (1.5.x): the delay is derived from the target
    // instant in the database, against the database's own clock, and rounded up to whole seconds.
    private static final String SQL_SET_VT_ONE_AT_FALLBACK =
            "select msg_id from pgmq.set_vt(?::text, ?::bigint, "
                    + "greatest(0, ceil(extract(epoch from (?::timestamptz - clock_timestamp()))))::integer)";

    private static final String SQL_SET_VT_MANY_AT =
            "select msg_id from pgmq.set_vt(?::text, ?::bigint[], ?::timestamptz)";

    private static final String SQL_POP_WITH_QTY = "select * from pgmq.pop(?::text, ?::integer)";

    private static final String SQL_POP = "select * from pgmq.pop(?::text)";

    private static final String SQL_DELETE_ONE = "select pgmq.delete(?::text, ?::bigint)";

    private static final String SQL_DELETE_MANY = "select * from pgmq.delete(?::text, ?::bigint[])";

    private static final String SQL_ARCHIVE_ONE = "select pgmq.archive(?::text, ?::bigint)";

    private static final String SQL_ARCHIVE_MANY = "select * from pgmq.archive(?::text, ?::bigint[])";

    private static final String SQL_SET_VT_ONE = "select msg_id from pgmq.set_vt(?::text, ?::bigint, ?::integer)";

    private static final String SQL_SET_VT_MANY = "select msg_id from pgmq.set_vt(?::text, ?::bigint[], ?::integer)";

    private static final String SQL_CREATE = "select pgmq.create(?::text)";

    private static final String SQL_CREATE_UNLOGGED = "select pgmq.create_unlogged(?::text)";

    private static final String SQL_CREATE_PARTITIONED =
            "select pgmq.create_partitioned(?::text, ?::text, ?::text)";

    private static final String SQL_DROP = "select pgmq.drop_queue(?::text)";

    private static final String SQL_PURGE = "select pgmq.purge_queue(?::text)";

    private static final String SQL_LIST_QUEUES = "select * from pgmq.list_queues()";

    private static final String SQL_METRICS = "select * from pgmq.metrics(?::text)";

    private static final String SQL_METRICS_ALL = "select * from pgmq.metrics_all()";

    private static final String SQLSTATE_UNDEFINED_TABLE = "42P01";

    private static final String SQLSTATE_UNDEFINED_FUNCTION = "42883";

    private static final String SQLSTATE_INVALID_TEXT_REPRESENTATION = "22P02";

    private final JdbcTemplate jdbcTemplate;

    private final PayloadConverter payloadConverter;

    private volatile @Nullable PgmqCapabilities capabilities;

    private volatile PgmqClientListener clientListener = new PgmqClientListener() {
    };

    private volatile @Nullable Integer maxBatchSize;

    private volatile Map<String, Object> defaultHeaders = Map.of();

    /**
     * Creates a template on the given {@link DataSource}.
     *
     * @param dataSource the data source; calls join the caller's Spring transaction when one is
     *     active on this same data source
     * @param payloadConverter converts payloads to and from JSON
     */
    public PgmqTemplate(DataSource dataSource, PayloadConverter payloadConverter) {
        this(new JdbcTemplate(dataSource), payloadConverter);
    }

    /**
     * Creates a template on an existing {@link JdbcTemplate}.
     *
     * @param jdbcTemplate the template to issue statements through
     * @param payloadConverter converts payloads to and from JSON
     */
    public PgmqTemplate(JdbcTemplate jdbcTemplate, PayloadConverter payloadConverter) {
        Assert.notNull(jdbcTemplate, "jdbcTemplate must not be null");
        Assert.notNull(payloadConverter, "payloadConverter must not be null");
        this.jdbcTemplate = jdbcTemplate;
        this.payloadConverter = payloadConverter;
    }

    /** The underlying {@link JdbcTemplate}, for callers that need to issue their own statements. */
    public JdbcTemplate getJdbcTemplate() {
        return this.jdbcTemplate;
    }

    @Override
    public PayloadConverter getPayloadConverter() {
        return this.payloadConverter;
    }

    /**
     * Registers a producer-side observability hook.
     *
     * <p>Used by the Micrometer integration; applications rarely need to set this themselves.
     */
    public void setClientListener(PgmqClientListener clientListener) {
        this.clientListener = clientListener;
    }

    /**
     * Splits batch sends ({@code sendBatch}, {@code sendRawBatch}, {@code sendMessages}) longer
     * than this into several {@code send_batch} statements, which bounds the size of each
     * statement's JSON parameter and the work a single statement does.
     *
     * <p>The chunks of one call stay atomic: they join the caller's transaction when one is bound
     * to this template's {@code DataSource}, and otherwise run in a transaction of their own. Ids
     * come back in input order. {@code null}, the default, sends every batch as one statement.
     *
     * @param maxBatchSize the most messages per statement, at least 1, or {@code null}
     */
    public void setMaxBatchSize(@Nullable Integer maxBatchSize) {
        Assert.isTrue(maxBatchSize == null || maxBatchSize >= 1,
                () -> "maxBatchSize must be at least 1, or null, but was " + maxBatchSize);
        this.maxBatchSize = maxBatchSize;
    }

    /** The most messages sent per statement, or {@code null} when batches are never split. */
    public @Nullable Integer getMaxBatchSize() {
        return this.maxBatchSize;
    }

    /**
     * Headers added to every message this template sends - single sends and every message of a
     * batch - such as the sending service or a schema version. A header of the same name in the
     * {@link SendOptions}, or on an {@link OutboundMessage}, wins.
     *
     * <p>Dead-lettering sends through the same template, so a dead-lettered message also gets any
     * default header its original did not already carry.
     *
     * @param defaultHeaders header names and JSON-serializable values; empty for none
     */
    public void setDefaultHeaders(Map<String, ?> defaultHeaders) {
        Assert.notNull(defaultHeaders, "defaultHeaders must not be null");
        this.defaultHeaders = Map.copyOf(defaultHeaders);
    }

    /** The headers added to every message sent, unless overridden. */
    public Map<String, Object> getDefaultHeaders() {
        return this.defaultHeaders;
    }

    // ---------------------------------------------------------------------
    // Queue management
    // ---------------------------------------------------------------------

    @Override
    public void createQueue(String queue) {
        createQueue(queue, QueueKind.STANDARD);
    }

    @Override
    public void createQueue(String queue, QueueKind kind) {
        QueueNames.validate(queue);
        String sql = switch (kind) {
            case STANDARD -> SQL_CREATE;
            case UNLOGGED -> SQL_CREATE_UNLOGGED;
            case PARTITIONED -> null;
        };
        if (sql == null) {
            // create_partitioned's interval defaults live in PGMQ, so call the 1-arg-equivalent
            // form by passing PGMQ's own documented defaults rather than inventing our own.
            createPartitionedQueue(queue, "10000", "100000");
            return;
        }
        execute(queue, sql, queue);
    }

    @Override
    public void createPartitionedQueue(String queue, String partitionInterval, String retentionInterval) {
        QueueNames.validate(queue);
        execute(queue, SQL_CREATE_PARTITIONED, queue, partitionInterval, retentionInterval);
    }

    @Override
    public boolean dropQueue(String queue) {
        QueueNames.validate(queue);
        Boolean dropped = this.jdbcTemplate.queryForObject(SQL_DROP, Boolean.class, queue);
        return Boolean.TRUE.equals(dropped);
    }

    @Override
    public long purgeQueue(String queue) {
        QueueNames.validate(queue);
        Long purged = queryOne(queue, SQL_PURGE, Long.class, queue);
        return purged != null ? purged : 0L;
    }

    @Override
    public boolean queueExists(String queue) {
        QueueNames.validate(queue);
        String normalized = QueueNames.normalize(queue);
        return listQueues().stream().anyMatch((info) -> QueueNames.normalize(info.name()).equals(normalized));
    }

    @Override
    public List<QueueInfo> listQueues() {
        return this.jdbcTemplate.query(SQL_LIST_QUEUES, (rs, rowNum) -> new QueueInfo(
                rs.getString("queue_name"),
                rs.getBoolean("is_partitioned"),
                rs.getBoolean("is_unlogged"),
                offsetDateTime(rs, "created_at")));
    }

    @Override
    public void createFifoIndex(String queue) {
        QueueNames.validate(queue);
        requireGroupedReads();
        execute(queue, SQL_CREATE_FIFO_INDEX, queue);
    }

    @Override
    public void enableNotifyInsert(String queue) {
        QueueNames.validate(queue);
        requireInsertNotify();
        execute(queue, SQL_ENABLE_NOTIFY_INSERT, QueueNames.normalize(queue));
    }

    @Override
    public void enableNotifyInsert(String queue, Duration throttle) {
        QueueNames.validate(queue);
        Assert.isTrue(throttle != null && !throttle.isNegative(), "throttle must not be negative");
        requireInsertNotify();
        long millis = throttle.toMillis() + (throttle.toNanosPart() % 1_000_000 > 0 ? 1 : 0);
        execute(queue, SQL_ENABLE_NOTIFY_INSERT_THROTTLED, QueueNames.normalize(queue),
                (int) Math.min(Integer.MAX_VALUE, millis));
    }

    @Override
    public void disableNotifyInsert(String queue) {
        QueueNames.validate(queue);
        if (!capabilities().insertNotify()) {
            return;
        }
        execute(queue, SQL_DISABLE_NOTIFY_INSERT, QueueNames.normalize(queue));
    }

    @Override
    public @Nullable Duration notifyInsertThrottle(String queue) {
        QueueNames.validate(queue);
        if (!capabilities().insertNotify()) {
            return null;
        }
        String normalized = QueueNames.normalize(queue);
        try {
            List<Integer> throttle = this.jdbcTemplate.queryForList(SQL_NOTIFY_INSERT_THROTTLE, Integer.class,
                    normalized, "q_" + normalized);
            return throttle.isEmpty() ? null : Duration.ofMillis(throttle.get(0));
        }
        catch (DataAccessException ex) {
            throw translate(queue, ex);
        }
    }

    private void requireInsertNotify() {
        PgmqCapabilities detected = capabilities();
        detected.require(detected.insertNotify(), "insert notifications (pgmq.enable_notify_insert)",
                new PgmqVersion(1, 10, 0));
    }

    @Override
    public QueueMetrics metrics(String queue) {
        QueueNames.validate(queue);
        try {
            List<QueueMetrics> results = this.jdbcTemplate.query(SQL_METRICS, metricsMapper(), queue);
            if (results.isEmpty()) {
                throw new QueueNotFoundException(queue, null);
            }
            return results.get(0);
        }
        catch (DataAccessException ex) {
            throw translate(queue, ex);
        }
    }

    @Override
    public List<QueueMetrics> metricsAll() {
        return this.jdbcTemplate.query(SQL_METRICS_ALL, metricsMapper());
    }

    private RowMapper<QueueMetrics> metricsMapper() {
        return (rs, rowNum) -> {
            Set<String> columns = columnNames(rs);
            return new QueueMetrics(
                    rs.getString("queue_name"),
                    rs.getLong("queue_length"),
                    nullableInt(rs, "newest_msg_age_sec"),
                    nullableInt(rs, "oldest_msg_age_sec"),
                    rs.getLong("total_messages"),
                    offsetDateTime(rs, "scrape_time"),
                    rs.getLong("queue_visible_length"),
                    columns.contains("default_partition_length") ? nullableLong(rs, "default_partition_length") : null);
        };
    }

    // ---------------------------------------------------------------------
    // Sending
    // ---------------------------------------------------------------------

    @Override
    public long send(String queue, Object payload) {
        return send(queue, payload, SendOptions.none());
    }

    @Override
    public long send(String queue, Object payload, SendOptions options) {
        Assert.notNull(payload, "payload must not be null");
        return sendRaw(queue, this.payloadConverter.toJson(payload), options);
    }

    @Override
    public long sendRaw(String queue, String json, SendOptions options) {
        QueueNames.validate(queue);
        Assert.hasText(json, "json must not be empty");
        String headers = headersJson(options);
        long startedAt = System.nanoTime();
        Long id;
        if (options.getDeliverAt() != null) {
            id = queryOne(queue, SQL_SEND_DELIVER_AT, Long.class,
                    queue, json, headers, timestamptz(options.getDeliverAt()));
        }
        else {
            id = queryOne(queue, SQL_SEND_DELAY_SECONDS, Long.class,
                    queue, json, headers, delaySeconds(options));
        }
        if (id == null) {
            throw new PgmqException("pgmq.send returned no message id for queue '" + queue + "'");
        }
        notifySent(queue, List.of(id), startedAt);
        return id;
    }

    @Override
    public List<Long> sendBatch(String queue, List<?> payloads) {
        return sendBatch(queue, payloads, SendOptions.none());
    }

    @Override
    public List<Long> sendBatch(String queue, List<?> payloads, SendOptions options) {
        List<String> json = payloads.stream().map(this.payloadConverter::toJson).toList();
        return sendRawBatch(queue, json, options);
    }

    @Override
    public List<Long> sendRawBatch(String queue, List<String> jsonPayloads, SendOptions options) {
        QueueNames.validate(queue);
        if (jsonPayloads.isEmpty()) {
            return List.of();
        }
        jsonPayloads.forEach((json) -> Assert.hasText(json, "json payloads must not be empty"));
        // PGMQ requires the headers array to be either null or the same length as the payloads.
        List<String> headers = null;
        Map<String, Object> shared = withDefaultHeaders(options.getHeaders());
        if (!shared.isEmpty()) {
            String single = this.payloadConverter.toJson(shared);
            headers = jsonPayloads.stream().map((ignored) -> single).toList();
        }
        return sendBatchStatements(queue, jsonPayloads, headers, options);
    }

    @Override
    public List<Long> sendMessages(String queue, List<OutboundMessage> messages) {
        return sendMessages(queue, messages, SendOptions.none());
    }

    @Override
    public List<Long> sendMessages(String queue, List<OutboundMessage> messages, SendOptions options) {
        QueueNames.validate(queue);
        Assert.notNull(messages, "messages must not be null");
        if (messages.isEmpty()) {
            return List.of();
        }
        List<String> payloads = new ArrayList<>(messages.size());
        List<String> headers = new ArrayList<>(messages.size());
        boolean anyHeaders = false;
        Map<String, Object> shared = withDefaultHeaders(options.getHeaders());
        for (OutboundMessage message : messages) {
            Assert.notNull(message, "messages must not contain null");
            payloads.add(message.json() ? (String) message.payload() : this.payloadConverter.toJson(message.payload()));
            Map<String, Object> merged = shared;
            if (!message.headers().isEmpty()) {
                // Shared headers are defaults; the message's own value wins on a name clash.
                merged = new LinkedHashMap<>(shared);
                merged.putAll(message.headers());
            }
            if (merged.isEmpty()) {
                headers.add("null");
            }
            else {
                headers.add(this.payloadConverter.toJson(merged));
                anyHeaders = true;
            }
        }
        return sendBatchStatements(queue, payloads, anyHeaders ? headers : null, options);
    }

    private List<Long> sendBatchStatements(String queue, List<String> payloads, @Nullable List<String> headers,
            SendOptions options) {
        Integer chunkSize = this.maxBatchSize;
        if (chunkSize == null || payloads.size() <= chunkSize) {
            return sendBatchStatement(queue, jsonStringArrayOf(payloads), headers != null ? jsonArrayOf(headers) : null,
                    options);
        }
        return inOneTransaction(() -> {
            List<Long> ids = new ArrayList<>(payloads.size());
            for (int from = 0; from < payloads.size(); from += chunkSize) {
                int to = Math.min(from + chunkSize, payloads.size());
                ids.addAll(sendBatchStatement(queue, jsonStringArrayOf(payloads.subList(from, to)),
                        headers != null ? jsonArrayOf(headers.subList(from, to)) : null, options));
            }
            return ids;
        });
    }

    /**
     * Runs {@code statements} in the transaction bound to this template's {@code DataSource}, or in a
     * new one when none is, so that several statements are as atomic as one would have been.
     */
    private <T> T inOneTransaction(Supplier<T> statements) {
        DataSource dataSource = this.jdbcTemplate.getDataSource();
        if (dataSource == null || TransactionSynchronizationManager.hasResource(dataSource)) {
            return statements.get();
        }
        T result = new TransactionTemplate(new DataSourceTransactionManager(dataSource))
                .execute((status) -> statements.get());
        Assert.state(result != null, "the statements returned no result");
        return result;
    }

    private List<Long> sendBatchStatement(String queue, String payloadArray, @Nullable String headerArray,
            SendOptions options) {
        Object[] args = options.getDeliverAt() != null
                ? new Object[] {queue, payloadArray, headerArray, timestamptz(options.getDeliverAt())}
                : new Object[] {queue, payloadArray, headerArray, delaySeconds(options)};
        String sql = options.getDeliverAt() != null ? SQL_SEND_BATCH_DELIVER_AT : SQL_SEND_BATCH_DELAY_SECONDS;
        long startedAt = System.nanoTime();
        try {
            List<Long> ids = this.jdbcTemplate.query(sql, (rs, rowNum) -> rs.getLong(1), args);
            notifySent(queue, ids, startedAt);
            return ids;
        }
        catch (DataAccessException ex) {
            throw translate(queue, ex);
        }
    }

    // ---------------------------------------------------------------------
    // Reading
    // ---------------------------------------------------------------------

    @Override
    public List<PgmqMessage<String>> read(String queue, ReadOptions options) {
        return read(queue, options, String.class);
    }

    @Override
    public <T> List<PgmqMessage<T>> read(String queue, ReadOptions options, Class<T> payloadType) {
        QueueNames.validate(queue);
        int visibilitySeconds = seconds(options.getVisibilityTimeout());
        String conditional = options.getConditional() != null
                ? this.payloadConverter.toJson(options.getConditional())
                : "{}";
        try {
            if (options.isLongPolling()) {
                Duration longPoll = options.getLongPoll();
                Duration interval = options.getPollInterval();
                return convertAll(queue, payloadType, this.jdbcTemplate.query(
                        SQL_READ_WITH_POLL,
                        rawMapper(queue),
                        queue,
                        visibilitySeconds,
                        options.getBatchSize(),
                        longPoll != null ? seconds(longPoll) : 0,
                        pollIntervalMillis(interval),
                        conditional));
            }
            return convertAll(queue, payloadType, this.jdbcTemplate.query(
                    SQL_READ, rawMapper(queue), queue, visibilitySeconds,
                    options.getBatchSize(), conditional));
        }
        catch (DataAccessException ex) {
            throw translate(queue, ex);
        }
    }

    @Override
    public List<PgmqMessage<String>> readGrouped(String queue, ReadOptions options) {
        return readGrouped(queue, options, String.class);
    }

    @Override
    public <T> List<PgmqMessage<T>> readGrouped(String queue, ReadOptions options, Class<T> payloadType) {
        QueueNames.validate(queue);
        if (options.getConditional() != null) {
            throw new IllegalArgumentException(
                    "readGrouped() does not support a conditional filter: PGMQ's grouped-read functions "
                            + "take no conditional argument. Filter after reading, or use read() instead.");
        }
        requireGroupedReads();
        int visibilitySeconds = seconds(options.getVisibilityTimeout());
        Duration longPoll = options.getLongPoll();
        try {
            if (longPoll != null) {
                Duration interval = options.getPollInterval();
                return convertAll(queue, payloadType, this.jdbcTemplate.query(
                        pollingStatementFor(options.getGroupStrategy()),
                        rawMapper(queue),
                        queue,
                        visibilitySeconds,
                        options.getBatchSize(),
                        seconds(longPoll),
                        pollIntervalMillis(interval)));
            }
            return convertAll(queue, payloadType, this.jdbcTemplate.query(
                    statementFor(options.getGroupStrategy()), rawMapper(queue),
                    queue, visibilitySeconds, options.getBatchSize()));
        }
        catch (DataAccessException ex) {
            throw translate(queue, ex);
        }
    }

    private static String statementFor(GroupReadStrategy strategy) {
        return switch (strategy) {
            case GREEDY -> SQL_READ_GROUPED;
            case HEAD -> SQL_READ_GROUPED_HEAD;
            case ROUND_ROBIN -> SQL_READ_GROUPED_RR;
        };
    }

    private static String pollingStatementFor(GroupReadStrategy strategy) {
        return switch (strategy) {
            case GREEDY -> SQL_READ_GROUPED_POLL;
            case HEAD -> SQL_READ_GROUPED_HEAD_POLL;
            case ROUND_ROBIN -> SQL_READ_GROUPED_RR_POLL;
        };
    }

    private void requireGroupedReads() {
        PgmqCapabilities detected = capabilities();
        detected.require(detected.groupedReads(), "grouped reads (pgmq.read_grouped)",
                new PgmqVersion(1, 10, 0));
    }

    @Override
    public <T> List<PgmqMessage<T>> pop(String queue, int count, Class<T> payloadType) {
        QueueNames.validate(queue);
        if (count < 1) {
            throw new IllegalArgumentException("count must be at least 1, but was " + count);
        }
        try {
            if (capabilities().popWithQuantity()) {
                return convertAll(queue, payloadType,
                        this.jdbcTemplate.query(SQL_POP_WITH_QTY, rawMapper(queue), queue, count));
            }
            // PGMQ before 1.7 pops exactly one message per call.
            List<PgmqMessage<String>> popped = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                List<PgmqMessage<String>> one = this.jdbcTemplate.query(SQL_POP, rawMapper(queue), queue);
                if (one.isEmpty()) {
                    break;
                }
                popped.addAll(one);
            }
            return convertAll(queue, payloadType, popped);
        }
        catch (DataAccessException ex) {
            throw translate(queue, ex);
        }
    }

    // ---------------------------------------------------------------------
    // Acknowledgement
    // ---------------------------------------------------------------------

    @Override
    public boolean delete(String queue, long messageId) {
        QueueNames.validate(queue);
        return Boolean.TRUE.equals(queryOne(queue, SQL_DELETE_ONE, Boolean.class, queue, messageId));
    }

    @Override
    public List<Long> delete(String queue, Collection<Long> messageIds) {
        return idOperation(queue, SQL_DELETE_MANY, messageIds);
    }

    @Override
    public boolean archive(String queue, long messageId) {
        QueueNames.validate(queue);
        return Boolean.TRUE.equals(queryOne(queue, SQL_ARCHIVE_ONE, Boolean.class, queue, messageId));
    }

    @Override
    public List<Long> archive(String queue, Collection<Long> messageIds) {
        return idOperation(queue, SQL_ARCHIVE_MANY, messageIds);
    }

    @Override
    public void setVisibilityTimeout(String queue, long messageId, Duration timeout) {
        QueueNames.validate(queue);
        int seconds = seconds(timeout);
        try {
            this.jdbcTemplate.query(SQL_SET_VT_ONE, (rs, rowNum) -> rs.getLong(1), queue, messageId, seconds);
        }
        catch (DataAccessException ex) {
            throw translate(queue, ex);
        }
    }

    @Override
    public void setVisibilityTimeout(String queue, Collection<Long> messageIds, Duration timeout) {
        QueueNames.validate(queue);
        if (messageIds.isEmpty()) {
            return;
        }
        int seconds = seconds(timeout);
        if (!capabilities().setVisibilityTimeoutBatch()) {
            for (Long id : messageIds) {
                setVisibilityTimeout(queue, id, timeout);
            }
            return;
        }
        try {
            this.jdbcTemplate.query(
                    SQL_SET_VT_MANY, (rs, rowNum) -> rs.getLong(1), queue, idArray(messageIds), seconds);
        }
        catch (DataAccessException ex) {
            throw translate(queue, ex);
        }
    }

    @Override
    public void setVisibleAt(String queue, long messageId, Instant visibleAt) {
        QueueNames.validate(queue);
        Assert.notNull(visibleAt, "visibleAt must not be null");
        String sql = capabilities().setVisibleAt() ? SQL_SET_VT_ONE_AT : SQL_SET_VT_ONE_AT_FALLBACK;
        try {
            this.jdbcTemplate.query(sql, (rs, rowNum) -> rs.getLong(1),
                    queue, messageId, timestamptz(visibleAt));
        }
        catch (DataAccessException ex) {
            throw translate(queue, ex);
        }
    }

    @Override
    public void setVisibleAt(String queue, Collection<Long> messageIds, Instant visibleAt) {
        QueueNames.validate(queue);
        if (messageIds.isEmpty()) {
            return;
        }
        PgmqCapabilities detected = capabilities();
        if (!detected.setVisibilityTimeoutBatch() || !detected.setVisibleAt()) {
            for (Long id : messageIds) {
                setVisibleAt(queue, id, visibleAt);
            }
            return;
        }
        try {
            this.jdbcTemplate.query(SQL_SET_VT_MANY_AT, (rs, rowNum) -> rs.getLong(1),
                    queue, idArray(messageIds), timestamptz(visibleAt));
        }
        catch (DataAccessException ex) {
            throw translate(queue, ex);
        }
    }

    // ---------------------------------------------------------------------
    // Capabilities
    // ---------------------------------------------------------------------

    @Override
    public PgmqCapabilities capabilities() {
        PgmqCapabilities current = this.capabilities;
        if (current == null) {
            synchronized (this) {
                current = this.capabilities;
                if (current == null) {
                    current = detectCapabilities();
                    this.capabilities = current;
                }
            }
        }
        return current;
    }

    /**
     * Probes the database for the PGMQ features it actually provides.
     *
     * <p>Called lazily by {@link #capabilities()} and eagerly by startup verification. The result
     * is derived from the catalog rather than from a version string, because a SQL-only PGMQ
     * installation has no {@code pg_extension} row to read a version from.
     *
     * @throws PgmqNotInstalledException if the {@code pgmq} schema is missing
     */
    public PgmqCapabilities detectCapabilities() {
        Boolean schemaPresent = this.jdbcTemplate.queryForObject(
                "select exists(select 1 from pg_namespace where nspname = 'pgmq')", Boolean.class);
        if (!Boolean.TRUE.equals(schemaPresent)) {
            throw new PgmqNotInstalledException(
                    "The 'pgmq' schema was not found in this database. Install PGMQ first - either "
                            + "CREATE EXTENSION pgmq; or PGMQ's SQL-only installation script.");
        }

        String versionString = null;
        boolean asExtension = false;
        List<String> versions = this.jdbcTemplate.queryForList(
                "select extversion from pg_extension where extname = 'pgmq'", String.class);
        if (!versions.isEmpty()) {
            versionString = versions.get(0);
            asExtension = true;
        }

        Set<String> signatures = new LinkedHashSet<>(this.jdbcTemplate.queryForList(
                "select p.proname || '(' || pg_get_function_arguments(p.oid) || ')' "
                        + "from pg_proc p join pg_namespace n on n.oid = p.pronamespace "
                        + "where n.nspname = 'pgmq'",
                String.class));
        Set<String> names = signatures.stream()
                .map((s) -> s.substring(0, s.indexOf('(')))
                .collect(Collectors.toCollection(LinkedHashSet::new));

        Set<String> messageColumns = new LinkedHashSet<>(this.jdbcTemplate.queryForList(
                "select a.attname from pg_type t "
                        + "join pg_class c on c.oid = t.typrelid "
                        + "join pg_attribute a on a.attrelid = c.oid "
                        + "join pg_namespace n on n.oid = t.typnamespace "
                        + "where n.nspname = 'pgmq' and t.typname = 'message_record' "
                        + "and a.attnum > 0 and not a.attisdropped",
                String.class));

        PgmqVersion version = PgmqVersion.parse(versionString);
        return new PgmqCapabilities(
                version,
                asExtension,
                messageColumns.contains("headers"),
                messageColumns.contains("last_read_at"),
                signatures.stream().anyMatch((s) -> s.startsWith("pop(") && s.contains("qty")),
                signatures.stream().anyMatch((s) -> s.startsWith("set_vt(") && s.contains("msg_ids")),
                signatures.stream().anyMatch((s) -> s.startsWith("set_vt(") && s.contains("timestamp with time zone")),
                names.contains("send_topic"),
                names.contains("read_grouped"),
                names.contains("enable_notify_insert"));
    }

    /**
     * Verifies that PGMQ is installed and new enough, throwing if it is not.
     *
     * @param minimumVersion the lowest acceptable version
     * @throws PgmqNotInstalledException if PGMQ is absent or older than {@code minimumVersion}
     */
    public PgmqCapabilities verifyInstallation(PgmqVersion minimumVersion) {
        return capabilities().verify(minimumVersion);
    }


    // ---------------------------------------------------------------------
    // Internals
    // ---------------------------------------------------------------------

    private void notifySent(String queue, List<Long> ids, long startedAtNanos) {
        try {
            this.clientListener.onSent(queue, ids, Duration.ofNanos(System.nanoTime() - startedAtNanos));
        }
        catch (RuntimeException ex) {
            LOGGER.warn("A PgmqClientListener callback failed for queue '" + queue + "'", ex);
        }
    }

    private List<Long> idOperation(String queue, String sql, Collection<Long> messageIds) {
        QueueNames.validate(queue);
        if (messageIds.isEmpty()) {
            return List.of();
        }
        try {
            return this.jdbcTemplate.query(sql, (rs, rowNum) -> rs.getLong(1), queue, idArray(messageIds));
        }
        catch (DataAccessException ex) {
            throw translate(queue, ex);
        }
    }

    /**
     * Maps a row to a message whose payload is still the raw JSON document.
     *
     * <p>Conversion deliberately happens <em>after</em> the result set has been read, never inside
     * the row mapper. By the time a row is being mapped PGMQ has already leased it and incremented
     * its read count; a converter throwing half-way through the result set would lose track of the
     * rows already leased and those not yet mapped, and the listener container could never see
     * the offending message to count it towards its attempt limit.
     */
    private RowMapper<PgmqMessage<String>> rawMapper(String queue) {
        return (rs, rowNum) -> {
            Set<String> columns = columnNames(rs);
            String raw = rs.getString("message");
            long id = rs.getLong("msg_id");
            String headerJson = columns.contains("headers") ? rs.getString("headers") : null;
            return new PgmqMessage<>(
                    id,
                    rs.getInt("read_ct"),
                    offsetDateTime(rs, "enqueued_at"),
                    columns.contains("last_read_at") ? nullableOffsetDateTime(rs, "last_read_at") : null,
                    offsetDateTime(rs, "vt"),
                    parseHeaders(queue, id, headerJson),
                    raw != null ? raw : "null",
                    raw != null ? raw : "null",
                    queue);
        };
    }

    @SuppressWarnings("unchecked")
    private <T> List<PgmqMessage<T>> convertAll(String queue, Class<T> payloadType, List<PgmqMessage<String>> raw) {
        if (payloadType == String.class) {
            return (List<PgmqMessage<T>>) (List<?>) raw;
        }
        List<PgmqMessage<T>> converted = new ArrayList<>(raw.size());
        for (PgmqMessage<String> message : raw) {
            converted.add(convert(message, payloadType));
        }
        return converted;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> PgmqMessage<T> convert(PgmqMessage<?> message, Class<T> payloadType) {
        if (payloadType == String.class) {
            return (PgmqMessage<T>) message.withPayload(message.rawPayload());
        }
        try {
            T payload = this.payloadConverter.fromJson(message.rawPayload(), payloadType);
            if (payload == null) {
                throw new PayloadConversionException("the payload is JSON null", null);
            }
            return message.withPayload(payload);
        }
        catch (RuntimeException ex) {
            throw new PayloadConversionException("Message " + message.id() + " on queue '" + message.queueName()
                    + "' could not be converted to " + payloadType.getName() + ": " + ex.getMessage(), ex);
        }
    }

    /**
     * Parses the headers column leniently.
     *
     * <p>PGMQ accepts any {@code jsonb} as headers, so a message written by another client may carry
     * an array or a scalar. That must not make the message unreadable - it would then fail every
     * delivery before a handler could ever see it - so a non-object is logged and dropped.
     */
    @SuppressWarnings("unchecked")
    private @Nullable Map<String, Object> parseHeaders(String queue, long id, @Nullable String headerJson) {
        if (headerJson == null || headerJson.isBlank() || "null".equals(headerJson)) {
            return null;
        }
        try {
            return this.payloadConverter.fromJson(headerJson, Map.class);
        }
        catch (RuntimeException ex) {
            // Log the shape, not the content: headers may carry data that does not belong in logs,
            // and a producer controls their size.
            LOGGER.warn("Ignoring headers of message " + id + " on queue '" + queue + "': they are not a JSON "
                    + "object (" + headerJson.length() + " characters, starting '"
                    + headerJson.substring(0, Math.min(16, headerJson.length())) + "')");
            return null;
        }
    }

    private @Nullable String headersJson(SendOptions options) {
        Map<String, Object> headers = withDefaultHeaders(options.getHeaders());
        return !headers.isEmpty() ? this.payloadConverter.toJson(headers) : null;
    }

    private Map<String, Object> withDefaultHeaders(Map<String, Object> headers) {
        Map<String, Object> defaults = this.defaultHeaders;
        if (defaults.isEmpty()) {
            return headers;
        }
        Map<String, Object> merged = new LinkedHashMap<>(defaults);
        merged.putAll(headers);
        return merged;
    }

    private static int delaySeconds(SendOptions options) {
        Duration delay = options.getDelay();
        return delay != null ? seconds(delay) : 0;
    }

    /**
     * Converts a duration to the whole seconds PGMQ takes, rounding <em>up</em>.
     *
     * <p>Truncating would turn a 500 ms visibility timeout into zero - a message visible to every
     * other consumer the instant it is read - so a fractional second always rounds up. Negative
     * durations become zero, and durations beyond {@code Integer.MAX_VALUE} seconds saturate
     * rather than overflow into a negative number.
     */
    static int seconds(Duration duration) {
        if (duration.isNegative() || duration.isZero()) {
            return 0;
        }
        long whole = duration.getSeconds() + (duration.getNano() > 0 ? 1 : 0);
        return (int) Math.min(Integer.MAX_VALUE, whole);
    }

    private static int pollIntervalMillis(@Nullable Duration interval) {
        long millis = interval != null ? interval.toMillis() : 100;
        return (int) Math.max(1, Math.min(Integer.MAX_VALUE, millis));
    }

    /** Binds an instant as a {@code timestamptz} with an explicit offset, independent of the JVM zone. */
    private static OffsetDateTime timestamptz(Instant instant) {
        return instant.atOffset(ZoneOffset.UTC);
    }

    /** Wraps already-encoded JSON documents into a single JSON array document. */
    private static String jsonArrayOf(List<String> jsonDocuments) {
        return jsonDocuments.stream().collect(Collectors.joining(",", "[", "]"));
    }

    /**
     * Encodes each document as a JSON string and wraps them in a JSON array, so the database can
     * parse every document individually. Used for payloads, which may come from the caller.
     */
    static String jsonStringArrayOf(List<String> jsonDocuments) {
        StringBuilder out = new StringBuilder(jsonDocuments.stream().mapToInt(String::length).sum() + 16);
        out.append('[');
        for (int i = 0; i < jsonDocuments.size(); i++) {
            if (i > 0) {
                out.append(',');
            }
            appendJsonString(out, jsonDocuments.get(i));
        }
        return out.append(']').toString();
    }

    /** Appends {@code value} as a JSON string literal, escaping per RFC 8259. */
    private static void appendJsonString(StringBuilder out, String value) {
        out.append('"');
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"' -> out.append("\\\"");
                case '\\' -> out.append("\\\\");
                case '\n' -> out.append("\\n");
                case '\r' -> out.append("\\r");
                case '\t' -> out.append("\\t");
                case '\b' -> out.append("\\b");
                case '\f' -> out.append("\\f");
                default -> {
                    if (c < 0x20) {
                        out.append(String.format("\\u%04x", (int) c));
                    }
                    else {
                        out.append(c);
                    }
                }
            }
        }
        out.append('"');
    }

    /** Renders message ids as a Postgres array literal. Safe: the values are {@code long}s. */
    private static String idArray(Collection<Long> ids) {
        return ids.stream().map(String::valueOf).collect(Collectors.joining(",", "{", "}"));
    }

    private void execute(String queue, String sql, Object... args) {
        try {
            this.jdbcTemplate.query(sql, (rs) -> null, args);
        }
        catch (DataAccessException ex) {
            throw translate(queue, ex);
        }
    }

    private <T> @Nullable T queryOne(String queue, String sql, Class<T> type, Object... args) {
        try {
            return this.jdbcTemplate.queryForObject(sql, type, args);
        }
        catch (EmptyResultDataAccessException ex) {
            return null;
        }
        catch (DataAccessException ex) {
            throw translate(queue, ex);
        }
    }

    /**
     * Turns Postgres errors into the library's typed exceptions.
     *
     * <p>PGMQ reports "no such queue" as a missing relation, which Spring surfaces as a
     * {@link BadSqlGrammarException}; translating it keeps callers from having to parse SQL state.
     */
    private PgmqException translate(String queue, DataAccessException ex) {
        String message = ex.getMostSpecificCause().getMessage();
        String sqlState = sqlState(ex);
        // SQLSTATE first: it is stable across Postgres versions and locales, unlike the message.
        if (SQLSTATE_UNDEFINED_TABLE.equals(sqlState)) {
            // Only the queue's own table missing means the queue does not exist. Its archive table
            // (pgmq.a_<queue>) can be missing on its own - dropped by hand, say - while the queue is
            // intact, and reporting that as "queue not found" would send the caller the wrong way.
            if (message == null || namesQueueTable(message, queue)) {
                return new QueueNotFoundException(queue, ex);
            }
            return new PgmqException("PGMQ operation failed on queue '" + queue + "': a table PGMQ needs is "
                    + "missing although the queue itself exists (" + message + ")", ex);
        }
        if (SQLSTATE_INVALID_TEXT_REPRESENTATION.equals(sqlState)) {
            return new PgmqException("PGMQ operation failed on queue '" + queue + "': a payload or header is not "
                    + "a single valid JSON document (" + message + ")", ex);
        }
        if (SQLSTATE_UNDEFINED_FUNCTION.equals(sqlState)) {
            return new PgmqException("PGMQ operation failed on queue '" + queue + "': a PGMQ function is missing, "
                    + "which usually means the installed PGMQ is older than this operation requires ("
                    + message + ")", ex);
        }
        if (message != null) {
            String lower = message.toLowerCase(java.util.Locale.ROOT);
            if (lower.contains("does not exist") && namesQueueTable(message, queue)) {
                return new QueueNotFoundException(queue, ex);
            }
            if (lower.contains("queue name is too long") || lower.contains("invalid characters")) {
                return new InvalidQueueNameException(queue, message);
            }
        }
        return new PgmqException("PGMQ operation failed on queue '" + queue + "': " + message, ex);
    }

    /** Whether a Postgres error message refers to the queue's own table, {@code pgmq.q_<queue>}. */
    private static boolean namesQueueTable(String message, String queue) {
        String lower = message.toLowerCase(java.util.Locale.ROOT);
        String table = "q_" + QueueNames.normalize(queue);
        return lower.contains("." + table + "\"") || lower.contains("\"" + table + "\"");
    }

    private static @Nullable String sqlState(Throwable ex) {
        for (Throwable current = ex; current != null; current = current.getCause()) {
            if (current instanceof SQLException sql && sql.getSQLState() != null) {
                return sql.getSQLState();
            }
        }
        return null;
    }

    private static Set<String> columnNames(ResultSet rs) throws SQLException {
        ResultSetMetaData metaData = rs.getMetaData();
        Set<String> names = new LinkedHashSet<>();
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            names.add(metaData.getColumnLabel(i).toLowerCase(java.util.Locale.ROOT));
        }
        return names;
    }

    private static OffsetDateTime offsetDateTime(ResultSet rs, String column) throws SQLException {
        OffsetDateTime value = nullableOffsetDateTime(rs, column);
        if (value == null) {
            throw new PgmqException("Expected column '" + column + "' to be non-null");
        }
        return value;
    }

    private static @Nullable OffsetDateTime nullableOffsetDateTime(ResultSet rs, String column) throws SQLException {
        OffsetDateTime direct = rs.getObject(column, OffsetDateTime.class);
        if (direct != null) {
            return direct;
        }
        Timestamp timestamp = rs.getTimestamp(column);
        return timestamp != null ? OffsetDateTime.ofInstant(timestamp.toInstant(), ZoneOffset.UTC) : null;
    }

    private static @Nullable Integer nullableInt(ResultSet rs, String column) throws SQLException {
        int value = rs.getInt(column);
        return rs.wasNull() ? null : value;
    }

    private static @Nullable Long nullableLong(ResultSet rs, String column) throws SQLException {
        long value = rs.getLong(column);
        return rs.wasNull() ? null : value;
    }
}
