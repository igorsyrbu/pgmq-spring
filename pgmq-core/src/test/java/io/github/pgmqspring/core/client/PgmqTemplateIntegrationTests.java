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
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.github.pgmqspring.core.InvalidQueueNameException;
import io.github.pgmqspring.core.PgmqCapabilities;
import io.github.pgmqspring.core.PgmqContainerSupport;
import io.github.pgmqspring.core.PgmqMessage;
import io.github.pgmqspring.core.QueueKind;
import io.github.pgmqspring.core.QueueMetrics;

import static io.github.pgmqspring.core.PgmqContainerSupport.countRows;
import static io.github.pgmqspring.core.PgmqContainerSupport.newQueue;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 * Integration tests for {@link PgmqTemplate} against a real PGMQ database.
 */
class PgmqTemplateIntegrationTests {

    private static PgmqTemplate pgmq;

    @BeforeAll
    static void setUp() {
        pgmq = PgmqContainerSupport.template();
    }

    record Order(String id, int quantity) {
    }

    @Test
    void sendReadDeleteRoundTrip() {
        String queue = newQueue("round_trip");

        long id = pgmq.send(queue, new Order("A-1", 3));
        assertThat(id).isPositive();

        List<PgmqMessage<Order>> messages = pgmq.read(queue, ReadOptions.defaults(), Order.class);

        assertThat(messages).singleElement().satisfies((message) -> {
            assertThat(message.id()).isEqualTo(id);
            assertThat(message.payload()).isEqualTo(new Order("A-1", 3));
            assertThat(message.readCount()).isEqualTo(1);
            assertThat(message.isRedelivered()).isFalse();
            assertThat(message.queueName()).isEqualTo(queue);
            assertThat(message.enqueuedAt()).isNotNull();
        });

        assertThat(pgmq.delete(queue, id)).isTrue();
        assertThat(pgmq.read(queue, ReadOptions.defaults(), Order.class)).isEmpty();
    }

    @Test
    void readMakesMessageInvisibleUntilVisibilityTimeoutExpires() {
        String queue = newQueue("visibility");
        pgmq.send(queue, new Order("B-1", 1));

        List<PgmqMessage<Order>> first = pgmq.read(
                queue, ReadOptions.defaults().visibilityTimeout(Duration.ofSeconds(30)), Order.class);
        assertThat(first).hasSize(1);

        // A second read within the visibility window must not see it.
        assertThat(pgmq.read(queue, ReadOptions.defaults(), Order.class)).isEmpty();

        // Releasing the lease makes it immediately deliverable again, with an incremented read count.
        pgmq.setVisibilityTimeout(queue, first.get(0).id(), Duration.ZERO);
        List<PgmqMessage<Order>> second = pgmq.read(queue, ReadOptions.defaults(), Order.class);
        assertThat(second).singleElement().satisfies((message) -> {
            assertThat(message.readCount()).isEqualTo(2);
            assertThat(message.isRedelivered()).isTrue();
        });
    }

    @Test
    void headersRoundTrip() {
        String queue = newQueue("headers");
        long id = pgmq.send(queue, new Order("C-1", 2),
                SendOptions.headers(Map.of("tenant", "acme", "attempt", 1)));

        PgmqMessage<Order> message = pgmq.read(queue, ReadOptions.defaults(), Order.class).get(0);

        assertThat(message.id()).isEqualTo(id);
        assertThat(message.headers()).containsEntry("tenant", "acme");
        assertThat(message.header("attempt")).isEqualTo(1);
    }

    @Test
    void messageWithoutHeadersHasNullHeaders() {
        String queue = newQueue("no_headers");
        pgmq.send(queue, new Order("C-2", 1));

        PgmqMessage<Order> message = pgmq.read(queue, ReadOptions.defaults(), Order.class).get(0);

        assertThat(message.headers()).isNull();
        assertThat(message.header("anything")).isNull();
    }

    @Test
    void sendRawStoresJsonVerbatimWithoutDoubleEncoding() {
        String queue = newQueue("raw_json");
        String json = "{\"already\":\"encoded\",\"nested\":{\"n\":1}}";

        pgmq.sendRaw(queue, json, SendOptions.none());

        PgmqMessage<String> message = pgmq.read(queue, ReadOptions.defaults()).get(0);

        // Had the payload been serialized again, this would be a quoted JSON string literal
        // such as "\"{\\\"already\\\"...\"" rather than an object.
        assertThat(message.rawPayload()).startsWith("{").contains("\"already\": \"encoded\"");
        assertThat(message.payload()).isEqualTo(message.rawPayload());
    }

    @Test
    void readWithStringTypeReturnsRawDocument() {
        String queue = newQueue("raw_read");
        pgmq.send(queue, new Order("D-1", 7));

        PgmqMessage<String> message = pgmq.read(queue, ReadOptions.defaults()).get(0);

        assertThat(message.payload()).contains("\"id\"").contains("D-1");
    }

    @Test
    void delayedMessageIsNotVisibleImmediately() {
        String queue = newQueue("delayed");
        pgmq.send(queue, new Order("E-1", 1), SendOptions.delayed(Duration.ofSeconds(60)));

        assertThat(pgmq.read(queue, ReadOptions.defaults(), Order.class)).isEmpty();
        assertThat(pgmq.metrics(queue).queueLength()).isEqualTo(1);
    }

    @Test
    void batchSendReturnsIdsInOrder() {
        String queue = newQueue("batch");
        List<Order> orders = List.of(new Order("F-1", 1), new Order("F-2", 2), new Order("F-3", 3));

        List<Long> ids = pgmq.sendBatch(queue, orders);

        assertThat(ids).hasSize(3).isSorted();

        List<PgmqMessage<Order>> read = pgmq.read(queue, ReadOptions.batch(10), Order.class);
        assertThat(read).extracting(PgmqMessage::payload).containsExactlyElementsOf(orders);
    }

    @Test
    void batchSendAppliesHeadersToEveryMessage() {
        String queue = newQueue("batch_headers");
        pgmq.sendBatch(queue, List.of(new Order("G-1", 1), new Order("G-2", 2)),
                SendOptions.headers(Map.of("source", "bulk")));

        List<PgmqMessage<Order>> read = pgmq.read(queue, ReadOptions.batch(10), Order.class);

        assertThat(read).hasSize(2).allSatisfy(
                (message) -> assertThat(message.headers()).containsEntry("source", "bulk"));
    }

    @Test
    void emptyBatchIsANoOp() {
        String queue = newQueue("empty_batch");
        assertThat(pgmq.sendBatch(queue, List.of())).isEmpty();
    }

    @Test
    void archiveMovesMessageOutOfQueue() {
        String queue = newQueue("archive");
        long id = pgmq.send(queue, new Order("H-1", 1));

        assertThat(pgmq.archive(queue, id)).isTrue();

        assertThat(pgmq.read(queue, ReadOptions.defaults(), Order.class)).isEmpty();
        assertThat(countRows("pgmq.a_" + queue)).isEqualTo(1);
    }

    @Test
    void batchDeleteAndArchiveReturnAffectedIds() {
        String queue = newQueue("batch_ack");
        List<Long> ids = pgmq.sendBatch(queue, List.of(new Order("I-1", 1), new Order("I-2", 2),
                new Order("I-3", 3), new Order("I-4", 4)));

        assertThat(pgmq.delete(queue, List.of(ids.get(0), ids.get(1)))).containsExactlyInAnyOrder(
                ids.get(0), ids.get(1));
        assertThat(pgmq.archive(queue, List.of(ids.get(2)))).containsExactly(ids.get(2));
        assertThat(pgmq.delete(queue, List.of())).isEmpty();
    }

    @Test
    void popReadsAndRemovesInOneStatement() {
        String queue = newQueue("pop");
        pgmq.sendBatch(queue, List.of(new Order("J-1", 1), new Order("J-2", 2)));

        List<PgmqMessage<Order>> popped = pgmq.pop(queue, 2, Order.class);

        assertThat(popped).hasSize(2);
        assertThat(pgmq.read(queue, ReadOptions.batch(10), Order.class)).isEmpty();
    }

    @Test
    void conditionalReadOnlyReturnsMatchingPayloads() {
        String queue = newQueue("conditional");
        pgmq.send(queue, Map.of("kind", "a", "n", 1));
        pgmq.send(queue, Map.of("kind", "b", "n", 2));

        List<PgmqMessage<String>> matching = pgmq.read(
                queue, ReadOptions.batch(10).conditional(Map.of("kind", "b")));

        assertThat(matching).singleElement()
                .satisfies((message) -> assertThat(message.rawPayload()).contains("\"b\""));
    }

    @Test
    void longPollReturnsPromptlyWhenAMessageIsAlreadyWaiting() {
        String queue = newQueue("long_poll");
        pgmq.send(queue, new Order("K-1", 1));

        long startedAt = System.nanoTime();
        List<PgmqMessage<Order>> messages = pgmq.read(
                queue, ReadOptions.defaults().longPoll(Duration.ofSeconds(5)), Order.class);
        Duration elapsed = Duration.ofNanos(System.nanoTime() - startedAt);

        assertThat(messages).hasSize(1);
        assertThat(elapsed).isLessThan(Duration.ofSeconds(5));
    }

    @Test
    void longPollReturnsEmptyAfterTheWindowExpires() {
        String queue = newQueue("long_poll_empty");

        long startedAt = System.nanoTime();
        List<PgmqMessage<Order>> messages = pgmq.read(
                queue, ReadOptions.defaults().longPoll(Duration.ofSeconds(1)), Order.class);
        Duration elapsed = Duration.ofNanos(System.nanoTime() - startedAt);

        assertThat(messages).isEmpty();
        assertThat(elapsed).isGreaterThanOrEqualTo(Duration.ofMillis(900));
    }

    @Test
    void queueLifecycleAndMetrics() {
        String queue = PgmqContainerSupport.uniqueQueueName("lifecycle");

        assertThat(pgmq.queueExists(queue)).isFalse();
        pgmq.createQueue(queue);
        assertThat(pgmq.queueExists(queue)).isTrue();
        assertThat(pgmq.listQueues()).anySatisfy((info) -> assertThat(info.name()).isEqualTo(queue));

        pgmq.sendBatch(queue, List.of(new Order("L-1", 1), new Order("L-2", 2)));

        QueueMetrics metrics = pgmq.metrics(queue);
        assertThat(metrics.queueName()).isEqualTo(queue);
        assertThat(metrics.queueLength()).isEqualTo(2);
        assertThat(metrics.totalMessages()).isEqualTo(2);
        assertThat(metrics.scrapeTime()).isNotNull();

        assertThat(pgmq.purgeQueue(queue)).isEqualTo(2);
        assertThat(pgmq.metrics(queue).queueLength()).isZero();

        assertThat(pgmq.metricsAll()).anySatisfy((m) -> assertThat(m.queueName()).isEqualTo(queue));
        assertThat(pgmq.dropQueue(queue)).isTrue();
        assertThat(pgmq.queueExists(queue)).isFalse();
    }

    @Test
    void createsUnloggedQueue() {
        String queue = PgmqContainerSupport.uniqueQueueName("unlogged");
        pgmq.createQueue(queue, QueueKind.UNLOGGED);

        assertThat(pgmq.listQueues())
                .filteredOn((info) -> info.name().equals(queue))
                .singleElement()
                .satisfies((info) -> {
                    assertThat(info.unlogged()).isTrue();
                    assertThat(info.kind()).isEqualTo(QueueKind.UNLOGGED);
                });
    }

    @Test
    void rejectsInvalidQueueNameBeforeIssuingSql() {
        assertThatExceptionOfType(InvalidQueueNameException.class)
                .isThrownBy(() -> pgmq.send("bad;name", "payload"));
    }

    @Test
    void detectsCapabilitiesOfTheRunningDatabase() {
        PgmqCapabilities capabilities = pgmq.capabilities();

        // Every supported PGMQ has headers; that is what defines our 1.5.0 floor.
        assertThat(capabilities.messageHeaders()).isTrue();
        assertThat(capabilities.installedAsExtension()).isTrue();
        assertThat(capabilities.version()).isNotNull();
        assertThat(capabilities.describe()).contains("PGMQ");
    }

    @Test
    void verifyInstallationAcceptsASupportedDatabase() {
        assertThat(pgmq.verifyInstallation(io.github.pgmqspring.core.PgmqVersion.MINIMUM_SUPPORTED)).isNotNull();
    }
}
