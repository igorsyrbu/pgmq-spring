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

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.jspecify.annotations.Nullable;

import io.github.pgmqspring.core.PgmqCapabilities;
import io.github.pgmqspring.core.PgmqMessage;
import io.github.pgmqspring.core.QueueInfo;
import io.github.pgmqspring.core.QueueKind;
import io.github.pgmqspring.core.QueueMetrics;
import io.github.pgmqspring.core.QueueNames;
import io.github.pgmqspring.core.convert.PayloadConverter;

/**
 * The PGMQ client: produce, consume, acknowledge and manage queues.
 *
 * <h2>Transaction participation</h2>
 *
 * <p>Every method runs on the JDBC {@code Connection} bound to the current Spring transaction when
 * one is active, and on a fresh pooled connection otherwise. This is what makes PGMQ a
 * transactional outbox with no extra infrastructure: a {@code send} inside an
 * {@code @Transactional} service method commits - or rolls back - atomically with the
 * application's own writes, because both are the same Postgres transaction.
 *
 * <pre>{@code
 * @Transactional
 * public void placeOrder(Order order) {
 *     orderRepository.save(order);
 *     pgmq.send("orders", new OrderPlaced(order.id()));
 *     // if this throws, the row AND the message both disappear
 * }
 * }</pre>
 *
 * <h2>Delivery semantics</h2>
 *
 * <p>PGMQ queues are <strong>competing-consumer</strong> queues with <strong>at-least-once</strong>
 * delivery. {@code pgmq.read()} selects rows {@code FOR UPDATE SKIP LOCKED}, so concurrent readers
 * never receive the same message, but a consumer that crashes after processing and before
 * acknowledging will see the message again once its visibility timeout expires. Handlers must be
 * idempotent.
 */
public interface PgmqOperations {

    // ---------------------------------------------------------------------
    // Queue management
    // ---------------------------------------------------------------------

    /** Creates a standard queue if it does not already exist. */
    void createQueue(String queue);

    /**
     * Creates a queue of the given kind if it does not already exist.
     *
     * @param queue the queue name
     * @param kind standard, unlogged or partitioned
     */
    void createQueue(String queue, QueueKind kind);

    /**
     * Creates a partitioned queue with explicit partitioning parameters.
     *
     * @param queue the queue name
     * @param partitionInterval PGMQ's {@code partition_interval} - either a number of message ids
     *     or a Postgres interval string
     * @param retentionInterval PGMQ's {@code retention_interval}
     */
    void createPartitionedQueue(String queue, String partitionInterval, String retentionInterval);

    /**
     * Drops a queue and its archive table, including every archived message.
     *
     * <p>To keep archived messages, copy them out of {@code pgmq.a_<queue>} first. PGMQ offers no
     * way to preserve the archive across a drop on every supported version: its
     * {@code detach_archive()} is a no-op in recent versions, and in older ones it makes the
     * subsequent {@code drop_queue()} fail.
     *
     * @return {@code true} if the queue existed and was dropped
     */
    boolean dropQueue(String queue);

    /**
     * Deletes every message in the queue.
     *
     * @return the number of messages removed
     */
    long purgeQueue(String queue);

    /** Whether a queue with this name exists. */
    boolean queueExists(String queue);

    /** Lists every queue PGMQ knows about. */
    List<QueueInfo> listQueues();


    /**
     * Creates the index PGMQ's grouped reads use to find each group's oldest message.
     *
     * <p>Not required for correctness - grouped reads work without it - but without it every poll
     * scans the queue table to group and rank messages. Idempotent, so it is safe to call on every
     * startup. Requires PGMQ 1.10.0 or later.
     *
     * @throws io.github.pgmqspring.core.UnsupportedPgmqFeatureException if the installed PGMQ
     *     does not provide grouped reads
     */
    void createFifoIndex(String queue);

    /**
     * Makes PGMQ send a notification on the channel {@link #notifyInsertChannel(String)} whenever a
     * message is inserted into this queue, with PGMQ's default throttle of 250 ms. A listener
     * container with {@code wakeUp(WakeUp.NOTIFY)} waits for it instead of polling.
     *
     * <p>Requires PGMQ 1.10.0 or later. Recreates the queue's notification trigger, which briefly
     * locks its table against inserts, so call it once rather than on every startup - or check
     * {@link #notifyInsertThrottle(String)} first.
     *
     * @throws io.github.pgmqspring.core.UnsupportedPgmqFeatureException if the installed PGMQ
     *     does not provide insert notifications
     */
    void enableNotifyInsert(String queue);

    /**
     * As {@link #enableNotifyInsert(String)}, sending at most one notification per {@code throttle}.
     *
     * <p>PGMQ <em>drops</em> the notifications of inserts within the throttle interval rather than
     * delaying them, so a consumer must not rely on one per message; the listener container keeps
     * polling briefly after every wake-up for that reason. Rounded up to whole milliseconds; zero
     * disables throttling.
     */
    void enableNotifyInsert(String queue, Duration throttle);

    /** Stops insert notifications for this queue. Does nothing when they are not enabled. */
    void disableNotifyInsert(String queue);

    /**
     * The throttle of this queue's insert notifications, or {@code null} when they are not enabled,
     * including on PGMQ versions that do not provide them.
     */
    @Nullable Duration notifyInsertThrottle(String queue);

    /** The channel PGMQ notifies on for inserts into this queue, as an unquoted identifier. */
    static String notifyInsertChannel(String queue) {
        return "pgmq.q_" + QueueNames.normalize(queue) + ".INSERT";
    }

    /** Returns depth and age metrics for one queue. */
    QueueMetrics metrics(String queue);

    /** Returns depth and age metrics for every queue. */
    List<QueueMetrics> metricsAll();

    // ---------------------------------------------------------------------
    // Sending
    // ---------------------------------------------------------------------

    /** Sends a payload, serialized with the configured converter. */
    long send(String queue, Object payload);

    /** Sends a payload with headers and/or a delay. */
    long send(String queue, Object payload, SendOptions options);

    /**
     * Sends an already-serialized JSON document, storing it verbatim.
     *
     * <p>Use this to avoid double-encoding when the payload is already JSON. Passing a JSON string
     * to {@link #send(String, Object)} would serialize it <em>again</em>, storing an escaped string
     * literal rather than the document.
     */
    long sendRaw(String queue, String json, SendOptions options);

    /** Sends several payloads in one statement, returning their ids in order. */
    List<Long> sendBatch(String queue, List<?> payloads);

    /** Sends several payloads in one statement with shared options. */
    List<Long> sendBatch(String queue, List<?> payloads, SendOptions options);

    /** Sends several already-serialized JSON documents in one statement. */
    List<Long> sendRawBatch(String queue, List<String> jsonPayloads, SendOptions options);

    /**
     * Sends several messages in one statement, each with its own headers, returning their ids in
     * order.
     *
     * <p>Unlike {@link #sendBatch(String, List, SendOptions)}, which gives every message the same
     * headers, this lets one batch mix FIFO group keys, trace ids or tenants.
     *
     * @see #sendMessages(String, List, SendOptions)
     */
    List<Long> sendMessages(String queue, List<OutboundMessage> messages);

    /**
     * Sends several messages in one statement, each with its own headers, plus shared options.
     *
     * <p>Headers in {@code options} are defaults for every message; a message's own header of the
     * same name wins. The delay or delivery time in {@code options} applies to the whole batch,
     * because PGMQ's {@code send_batch} takes a single one.
     */
    List<Long> sendMessages(String queue, List<OutboundMessage> messages, SendOptions options);

    // ---------------------------------------------------------------------
    // Reading
    // ---------------------------------------------------------------------

    /** Reads messages, leaving the payload as the raw JSON document. */
    List<PgmqMessage<String>> read(String queue, ReadOptions options);

    /**
     * Reads messages and deserializes each payload to {@code payloadType}.
     *
     * <p>Read messages become invisible to other consumers for the configured visibility timeout
     * and their read count is incremented. They remain in the queue until acknowledged with
     * {@link #delete(String, long)} or {@link #archive(String, long)}.
     */
    <T> List<PgmqMessage<T>> read(String queue, ReadOptions options, Class<T> payloadType);

    /**
     * Reads and deletes messages in a single statement.
     *
     * <p>There is no acknowledgement step and therefore no redelivery: if the application crashes
     * after the pop commits, the message is gone. This is at-most-once delivery.
     */
    <T> List<PgmqMessage<T>> pop(String queue, int count, Class<T> payloadType);

    // ---------------------------------------------------------------------
    // Acknowledgement
    // ---------------------------------------------------------------------

    /**
     * Reads messages honouring FIFO group ordering, leaving payloads as raw JSON documents.
     *
     * @see #readGrouped(String, ReadOptions, Class)
     */
    List<PgmqMessage<String>> readGrouped(String queue, ReadOptions options);

    /**
     * Reads messages honouring FIFO group ordering, deserializing each payload to
     * {@code payloadType}.
     *
     * <p>For every distinct {@link FifoGroups#GROUP_HEADER} value, PGMQ hands out messages in send
     * order and withholds the entire group while one of its messages is unacknowledged - so no two
     * consumers, however many are polling, can ever hold an unacknowledged message for the same
     * group. Messages sent without a group header share one implicit group and are ordered among
     * themselves too.
     *
     * <p>{@link ReadOptions#groupStrategy(GroupReadStrategy)} selects how a batch is spread across
     * groups. Long polling is supported for every strategy. A conditional filter is not: PGMQ's
     * grouped-read functions take no conditional argument.
     *
     * <p>Requires PGMQ 1.10.0 or later.
     *
     * @throws io.github.pgmqspring.core.UnsupportedPgmqFeatureException if the installed PGMQ
     *     does not provide grouped reads
     * @throws IllegalArgumentException if {@code options} carries a conditional filter
     */
    <T> List<PgmqMessage<T>> readGrouped(String queue, ReadOptions options, Class<T> payloadType);

    /**
     * Deletes one message.
     *
     * @return {@code true} if the message existed
     */
    boolean delete(String queue, long messageId);

    /**
     * Deletes several messages.
     *
     * @return the ids that were actually deleted
     */
    List<Long> delete(String queue, Collection<Long> messageIds);

    /**
     * Moves one message to the queue's archive table.
     *
     * @return {@code true} if the message existed
     */
    boolean archive(String queue, long messageId);

    /**
     * Moves several messages to the archive table.
     *
     * @return the ids that were actually archived
     */
    List<Long> archive(String queue, Collection<Long> messageIds);

    /**
     * Resets a message's visibility timeout.
     *
     * <p>Pass {@link Duration#ZERO} to make it immediately available again ("retry now"), or a
     * longer duration to extend the lease while a slow handler is still working.
     */
    void setVisibilityTimeout(String queue, long messageId, Duration timeout);

    /** Extends or resets the visibility timeout of several messages at once. */
    void setVisibilityTimeout(String queue, Collection<Long> messageIds, Duration timeout);

    /**
     * Makes a message invisible until a specific instant, rather than for a relative duration.
     *
     * <p>Useful when the retry time is a wall-clock decision rather than a backoff - "try again
     * after the nightly batch", "retry when the rate limit window resets at the top of the hour" -
     * where computing a {@link Duration} from now would drift. Backed by PGMQ's {@code timestamptz}
     * overload of {@code set_vt} where it exists; on PGMQ versions without it the delay is derived
     * from {@code visibleAt} inside the database, against its clock, rounded up to whole seconds.
     */
    void setVisibleAt(String queue, long messageId, Instant visibleAt);

    /** Makes several messages invisible until a specific instant. */
    void setVisibleAt(String queue, Collection<Long> messageIds, Instant visibleAt);

    /**
     * Counts a failed attempt on a message consumed with {@code pop} inside a transaction that
     * rolled back, and makes it visible again after {@code delay}, rounded up to whole seconds.
     *
     * <p>{@code read_ct} counts reads, and a pop is not one; a rolled-back pop leaves the message
     * exactly as it was - uncounted, and visible at once. This restores what a failed read would
     * have left behind, in one statement, so attempts survive restarts and are shared by every
     * instance as they are in read mode. PGMQ has no function for it, so it updates the queue's
     * table directly.
     */
    void retryAfterRollback(String queue, long messageId, Duration delay);

    // ---------------------------------------------------------------------
    // Introspection
    // ---------------------------------------------------------------------

    /**
     * What the PGMQ installation in the target database supports.
     *
     * <p>Detected once on first use and cached.
     */
    PgmqCapabilities capabilities();

    /** The converter this client uses to turn payloads into JSON and back. */
    PayloadConverter getPayloadConverter();

    /**
     * Converts a message's raw JSON payload into {@code payloadType}, keeping every other field.
     *
     * <p>Reading as {@code String} and converting afterwards is how the listener container keeps a
     * single malformed message from failing a whole batch: the failure is attributed to that one
     * message, which then counts towards its attempt limit like any other handler failure.
     *
     * @throws io.github.pgmqspring.core.convert.PayloadConversionException if the payload cannot
     *     be converted, or is JSON {@code null}
     */
    <T> PgmqMessage<T> convert(PgmqMessage<?> message, Class<T> payloadType);

    /** Reads the headers of a message as a map, or an empty map when it has none. */
    default Map<String, Object> headersOf(PgmqMessage<?> message) {
        Map<String, Object> headers = message.headers();
        return headers != null ? headers : Map.of();
    }
}
