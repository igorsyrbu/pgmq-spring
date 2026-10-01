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

package io.github.pgmqspring.core;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Map;

import org.jspecify.annotations.Nullable;

/**
 * A message read from a PGMQ queue.
 *
 * @param <T> the deserialized payload type
 * @param id the {@code msg_id}, unique within the queue
 * @param readCount how many times this message has been delivered, including the current delivery.
 *     PGMQ increments it on every read, which is what makes poison-message detection possible
 *     without extra bookkeeping
 * @param enqueuedAt when the message was written to the queue
 * @param lastReadAt when the message was last delivered; {@code null} on PGMQ versions older than
 *     1.10.0, which is the release that added the {@code last_read_at} column to
 *     {@code pgmq.message_record}
 * @param visibleAt the visibility deadline: until this instant the message is invisible to other
 *     consumers. If it is not acknowledged before then, it becomes deliverable again
 * @param headers the message headers, or {@code null} when the message was sent without any
 * @param payload the deserialized payload
 * @param rawPayload the payload exactly as stored in the {@code jsonb} column
 * @param queueName the queue this message came from
 */
public record PgmqMessage<T>(
        long id,
        int readCount,
        OffsetDateTime enqueuedAt,
        @Nullable OffsetDateTime lastReadAt,
        OffsetDateTime visibleAt,
        @Nullable Map<String, Object> headers,
        T payload,
        String rawPayload,
        String queueName) {

    /**
     * Returns how long remains before this message becomes visible to other consumers again.
     *
     * <p>A zero or negative duration means the lease has already expired and the message may be
     * redelivered at any moment; acknowledging it at that point is no longer safe to assume
     * exclusive.
     */
    public Duration remainingVisibility() {
        return Duration.between(Instant.now(), this.visibleAt.toInstant());
    }

    /** Returns {@code true} if this message has been delivered more than once. */
    public boolean isRedelivered() {
        return this.readCount > 1;
    }

    /** Returns the header value for {@code name}, or {@code null} if absent. */
    public @Nullable Object header(String name) {
        return this.headers != null ? this.headers.get(name) : null;
    }

    /**
     * Returns this message's FIFO group key, or {@code null} if it was sent without one.
     *
     * @see io.github.pgmqspring.core.client.FifoGroups
     */
    public @Nullable String groupKey() {
        Object value = header(io.github.pgmqspring.core.client.FifoGroups.GROUP_HEADER);
        return value != null ? value.toString() : null;
    }

    /** Returns a copy of this message with a different payload, preserving all metadata. */
    public <R> PgmqMessage<R> withPayload(R newPayload) {
        return new PgmqMessage<>(
                this.id,
                this.readCount,
                this.enqueuedAt,
                this.lastReadAt,
                this.visibleAt,
                this.headers,
                newPayload,
                this.rawPayload,
                this.queueName);
    }
}
