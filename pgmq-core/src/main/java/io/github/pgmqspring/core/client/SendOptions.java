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
import java.util.LinkedHashMap;
import java.util.Map;

import org.jspecify.annotations.Nullable;

/**
 * Options for a send operation: headers and an optional delivery delay.
 *
 * <p>A delay may be expressed either relatively ({@link #delay(Duration)}, mapped to PGMQ's
 * {@code integer} seconds overload) or absolutely ({@link #deliverAt(Instant)}, mapped to the
 * {@code timestamptz} overload). Setting one clears the other.
 *
 * <p>Instances are immutable; every mutator returns a new instance.
 */
public final class SendOptions {

    private static final SendOptions NONE = new SendOptions(Map.of(), null, null);

    private final Map<String, Object> headers;

    private final @Nullable Duration delay;

    private final @Nullable Instant deliverAt;

    private SendOptions(Map<String, Object> headers, @Nullable Duration delay, @Nullable Instant deliverAt) {
        this.headers = headers;
        this.delay = delay;
        this.deliverAt = deliverAt;
    }

    /** Default options: no headers, immediate delivery. */
    public static SendOptions none() {
        return NONE;
    }

    /** Options carrying the supplied headers. */
    public static SendOptions headers(Map<String, Object> headers) {
        return NONE.withHeaders(headers);
    }

    /** Options delaying delivery by {@code delay}. */
    public static SendOptions delayed(Duration delay) {
        return NONE.delay(delay);
    }

    /** Returns a copy with the supplied headers, replacing any already set. */
    public SendOptions withHeaders(Map<String, Object> newHeaders) {
        return new SendOptions(Map.copyOf(newHeaders), this.delay, this.deliverAt);
    }

    /**
     * Returns a copy assigning this message to a FIFO group.
     *
     * <p>Sets the {@link FifoGroups#GROUP_HEADER} header. While an earlier message in the same
     * group is unacknowledged, a grouped read will not hand this one to any consumer.
     *
     * @see PgmqOperations#readGrouped(String, ReadOptions)
     */
    public SendOptions group(String key) {
        return withHeader(FifoGroups.GROUP_HEADER, key);
    }

    /** Returns a copy with one additional header. */
    public SendOptions withHeader(String name, Object value) {
        Map<String, Object> merged = new LinkedHashMap<>(this.headers);
        merged.put(name, value);
        return new SendOptions(Map.copyOf(merged), this.delay, this.deliverAt);
    }

    /** Returns a copy delaying delivery by {@code newDelay}, clearing any absolute delivery time. */
    public SendOptions delay(Duration newDelay) {
        return new SendOptions(this.headers, newDelay, null);
    }

    /** Returns a copy delivering at {@code instant}, clearing any relative delay. */
    public SendOptions deliverAt(Instant instant) {
        return new SendOptions(this.headers, null, instant);
    }

    public Map<String, Object> getHeaders() {
        return this.headers;
    }

    public @Nullable Duration getDelay() {
        return this.delay;
    }

    public @Nullable Instant getDeliverAt() {
        return this.deliverAt;
    }

    /** Whether any header was set. */
    public boolean hasHeaders() {
        return !this.headers.isEmpty();
    }
}
