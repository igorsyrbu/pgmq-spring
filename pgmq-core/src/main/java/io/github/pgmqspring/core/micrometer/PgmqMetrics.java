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

package io.github.pgmqspring.core.micrometer;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import org.jspecify.annotations.Nullable;

import io.github.pgmqspring.core.PgmqMessage;
import io.github.pgmqspring.core.client.PgmqClientListener;
import io.github.pgmqspring.core.consumer.ConsumerListener;

/**
 * Micrometer instrumentation for PGMQ, implementing both the producer-side
 * {@link PgmqClientListener} and the consumer-side {@link ConsumerListener}.
 *
 * <p>Meter names follow Micrometer's convention: dot-separated, lower case, with the queue as a
 * tag rather than baked into the name so that queues can be aggregated.
 *
 * <table border="1">
 * <caption>Published meters</caption>
 * <tr><th>Meter</th><th>Type</th><th>Meaning</th></tr>
 * <tr><td>{@code pgmq.messages.sent}</td><td>counter</td><td>messages written to a queue</td></tr>
 * <tr><td>{@code pgmq.messages.received}</td><td>counter</td><td>messages delivered to a handler</td></tr>
 * <tr><td>{@code pgmq.messages.acknowledged}</td><td>counter</td><td>handlers that returned normally</td></tr>
 * <tr><td>{@code pgmq.messages.failed}</td><td>counter</td><td>handlers that threw</td></tr>
 * <tr><td>{@code pgmq.messages.dead.lettered}</td><td>counter</td><td>messages moved to a dead-letter queue</td></tr>
 * <tr><td>{@code pgmq.messages.poison}</td><td>counter</td><td>messages that exceeded max-attempts</td></tr>
 * <tr><td>{@code pgmq.poll.errors}</td><td>counter</td>
 *     <td>polls that failed, for example while the database is unreachable</td></tr>
 * <tr><td>{@code pgmq.send.duration}</td><td>timer</td><td>time spent in one send call</td></tr>
 * <tr><td>{@code pgmq.processing.duration}</td><td>timer</td><td>time spent in a handler</td></tr>
 * </table>
 *
 * <p>Queue depth and oldest-message age are published separately by {@link PgmqQueueGauges},
 * because they are polled from {@code pgmq.metrics()} rather than observed inline.
 */
public class PgmqMetrics implements ConsumerListener, PgmqClientListener {

    /** Tag key carrying the queue name. */
    public static final String TAG_QUEUE = "queue";

    private static final String SENT = "pgmq.messages.sent";

    private static final String RECEIVED = "pgmq.messages.received";

    private static final String ACKNOWLEDGED = "pgmq.messages.acknowledged";

    private static final String FAILED = "pgmq.messages.failed";

    private static final String DEAD_LETTERED = "pgmq.messages.dead.lettered";

    private static final String POISON = "pgmq.messages.poison";

    private static final String POLL_ERRORS = "pgmq.poll.errors";

    private static final String SEND_DURATION = "pgmq.send.duration";

    private static final String PROCESSING_DURATION = "pgmq.processing.duration";

    private final MeterRegistry registry;

    // Meters are resolved once per distinct name and tags, then reused: building and registering a
    // meter on every message costs a builder, a Tags list and a registry lookup on the hot path.
    private final Map<Key, Counter> counters = new ConcurrentHashMap<>();

    private final Map<Key, Timer> timers = new ConcurrentHashMap<>();

    public PgmqMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    @Override
    public void onSent(String queue, List<Long> messageIds, Duration duration) {
        counter(SENT, "Messages written to the queue", queue, null, null).increment(messageIds.size());
        timer(SEND_DURATION, "Time spent writing messages to a PGMQ queue", queue, null, null)
                .record(duration.toNanos(), TimeUnit.NANOSECONDS);
    }

    @Override
    public void onPolled(String queue, List<? extends PgmqMessage<?>> messages) {
        if (!messages.isEmpty()) {
            counter(RECEIVED, "Messages delivered to a handler", queue, null, null).increment(messages.size());
        }
    }

    @Override
    public void onSuccess(String queue, PgmqMessage<?> message, Duration duration) {
        counter(ACKNOWLEDGED, "Messages whose handler returned normally", queue, null, null).increment();
        recordProcessing(queue, "success", duration);
    }

    @Override
    public void onFailure(String queue, PgmqMessage<?> message, Duration duration, Throwable error) {
        counter(FAILED, "Messages whose handler threw", queue, "exception", exceptionTag(error))
                .increment();
        recordProcessing(queue, "failure", duration);
    }

    @Override
    public void onPoison(String queue, PgmqMessage<?> message) {
        counter(POISON, "Messages that exceeded the configured attempt limit", queue, null, null).increment();
    }

    @Override
    public void onDeadLettered(String queue, PgmqMessage<?> message, String deadLetterQueue, String reason) {
        counter(DEAD_LETTERED, "Messages moved to a dead-letter queue", queue, "dead.letter.queue", deadLetterQueue)
                .increment();
    }

    @Override
    public void onPollError(String queue, Throwable error) {
        counter(POLL_ERRORS, "Polls that failed, for example while the database is unreachable", queue,
                "exception", exceptionTag(error)).increment();
    }

    /** The exception's simple name; anonymous and lambda classes have none, so they get their full name. */
    private static String exceptionTag(Throwable error) {
        String simpleName = error.getClass().getSimpleName();
        return simpleName.isEmpty() ? error.getClass().getName() : simpleName;
    }

    private void recordProcessing(String queue, String outcome, Duration duration) {
        timer(PROCESSING_DURATION, "Time spent in a PGMQ message handler", queue, "outcome", outcome)
                .record(duration.toNanos(), TimeUnit.NANOSECONDS);
    }

    private Counter counter(String name, String description, String queue, @Nullable String tagKey,
            @Nullable String tagValue) {
        return this.counters.computeIfAbsent(new Key(name, queue, tagKey, tagValue),
                (key) -> Counter.builder(name).description(description).tags(key.tags()).register(this.registry));
    }

    private Timer timer(String name, String description, String queue, @Nullable String tagKey,
            @Nullable String tagValue) {
        return this.timers.computeIfAbsent(new Key(name, queue, tagKey, tagValue),
                (key) -> Timer.builder(name).description(description).tags(key.tags()).register(this.registry));
    }

    /** A meter's identity: its name, queue and at most one further tag. */
    private record Key(String name, String queue, @Nullable String tagKey, @Nullable String tagValue) {

        Tags tags() {
            Tags tags = Tags.of(TAG_QUEUE, this.queue);
            return this.tagKey != null && this.tagValue != null ? tags.and(this.tagKey, this.tagValue) : tags;
        }
    }
}
