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

package io.github.pgmqspring.core.consumer;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.github.pgmqspring.core.ListenerContainers;
import io.github.pgmqspring.core.PgmqContainerSupport;
import io.github.pgmqspring.core.RecordingOperations;
import io.github.pgmqspring.core.client.PgmqTemplate;

import static io.github.pgmqspring.core.PgmqContainerSupport.countRows;
import static io.github.pgmqspring.core.PgmqContainerSupport.newQueue;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.awaitility.Awaitility.await;

/**
 * {@link ConsumerOptions.Builder#batchAcknowledgements(boolean)}: how many statements acknowledge
 * a batch, and that a pending acknowledgement never lets its message reach another consumer.
 */
class BatchAcknowledgementsIntegrationTests {

    private static PgmqTemplate pgmq;

    @RegisterExtension
    final ListenerContainers containers = new ListenerContainers();

    @BeforeAll
    static void setUp() {
        pgmq = PgmqContainerSupport.template();
    }

    private static List<Map<String, Integer>> payloads(int count) {
        return IntStream.rangeClosed(1, count).mapToObj((n) -> Map.of("n", n)).toList();
    }

    @Test
    void aBatchOfTenIsDeletedWithOneStatement() {
        String queue = newQueue("batch_ack_delete");
        pgmq.sendBatch(queue, payloads(10));
        RecordingOperations recording = new RecordingOperations(pgmq);

        this.containers.start(PgmqMessageListenerContainer.builder(recording.proxy(), queue, String.class)
                .options(ConsumerOptions.builder()
                        .batchSize(10)
                        .batchAcknowledgements(true)
                        .pollDelay(Duration.ofMillis(50))
                        .build())
                .handler((message) -> { })
                .build());

        await().atMost(Duration.ofSeconds(20)).until(() -> countRows("pgmq.q_" + queue) == 0);
        assertThat(recording.acknowledgements()).containsExactly("delete[10]");
    }

    @Test
    void archivesWithOneStatementAndFlushesEarlyAtAckBatchSize() {
        String queue = newQueue("batch_ack_archive");
        pgmq.sendBatch(queue, payloads(10));
        RecordingOperations recording = new RecordingOperations(pgmq);

        this.containers.start(PgmqMessageListenerContainer.builder(recording.proxy(), queue, String.class)
                .options(ConsumerOptions.builder()
                        .batchSize(10)
                        .acknowledgeMode(AcknowledgeMode.ARCHIVE)
                        .batchAcknowledgements(true)
                        .ackBatchSize(4)
                        .pollDelay(Duration.ofMillis(50))
                        .build())
                .handler((message) -> { })
                .build());

        await().atMost(Duration.ofSeconds(20)).until(() -> countRows("pgmq.q_" + queue) == 0);
        assertThat(countRows("pgmq.a_" + queue)).isEqualTo(10);
        assertThat(recording.acknowledgements()).containsExactly("archive[4]", "archive[4]", "archive[2]");
    }

    @Test
    void aFailedFlushRedeliversTheBatchWithoutTreatingItAsAHandlerFailure() {
        String queue = newQueue("batch_ack_flush_fails");
        pgmq.sendBatch(queue, payloads(3));
        RecordingOperations recording = new RecordingOperations(pgmq);
        recording.failNext(1, (call) -> call.startsWith("delete[") || call.startsWith("archive["));
        ConcurrentLinkedQueue<Integer> readCounts = new ConcurrentLinkedQueue<>();

        this.containers.start(PgmqMessageListenerContainer.builder(recording.proxy(), queue, String.class)
                .options(ConsumerOptions.builder()
                        .batchSize(10)
                        .visibilityTimeout(Duration.ofSeconds(1))
                        .batchAcknowledgements(true)
                        .pollDelay(Duration.ofMillis(50))
                        .maxPollDelay(Duration.ofMillis(100))
                        .build())
                .handler((message) -> readCounts.add(message.readCount()))
                .build());

        await().atMost(Duration.ofSeconds(20)).until(() -> countRows("pgmq.q_" + queue) == 0);
        assertThat(readCounts).containsExactlyInAnyOrder(1, 1, 1, 2, 2, 2);
        assertThat(recording.acknowledgements()).containsExactly("delete[3]", "delete[3]");
        // No retry delay was applied: the handlers had succeeded.
        assertThat(recording.calls()).doesNotContain("setVisibilityTimeout");
    }

    @Test
    void aPendingAcknowledgementKeepsItsMessageLeased() {
        String queue = newQueue("batch_ack_lease");
        ConcurrentLinkedQueue<Integer> readCounts = new ConcurrentLinkedQueue<>();
        AtomicInteger invocations = new AtomicInteger();

        // The first message is handled at once, then waits ~4s for its batch-mates before its delete
        // is sent - longer than the 3s visibility timeout. Unless the lease keeps covering it while
        // it is pending, the second polling loop reads it again.
        this.containers.start(PgmqMessageListenerContainer.builder(pgmq, queue, String.class)
                .options(ConsumerOptions.builder()
                        .concurrency(2)
                        .batchSize(3)
                        .visibilityTimeout(Duration.ofSeconds(3))
                        .extendLease(true)
                        .batchAcknowledgements(true)
                        .pollDelay(Duration.ofMillis(50))
                        .maxPollDelay(Duration.ofMillis(100))
                        .build())
                .handler((message) -> {
                    readCounts.add(message.readCount());
                    if (invocations.incrementAndGet() > 1) {
                        Thread.sleep(2000);
                    }
                })
                .build());
        pgmq.sendBatch(queue, payloads(3));

        await().atMost(Duration.ofSeconds(30)).until(() -> countRows("pgmq.q_" + queue) == 0);
        await().during(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(5)).until(() -> readCounts.size() == 3);
        assertThat(readCounts).containsOnly(1);
    }

    @Test
    void rejectsCombinationsWhereTheAcknowledgementCannotBeDeferred() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> ConsumerOptions.builder().batchAcknowledgements(true).transactional(true).build())
                .withMessageContaining("batchAcknowledgements cannot be combined with transactional");
        assertThatIllegalArgumentException()
                .isThrownBy(() -> ConsumerOptions.builder()
                        .batchAcknowledgements(true).acknowledgeMode(AcknowledgeMode.MANUAL).build())
                .withMessageContaining("acknowledgeMode=MANUAL");
        assertThatIllegalArgumentException()
                .isThrownBy(() -> ConsumerOptions.builder().ackBatchSize(0).build())
                .withMessageContaining("ackBatchSize must be at least 1");
        ConsumerOptions options = ConsumerOptions.builder().batchAcknowledgements(true).ackBatchSize(5).build();
        assertThat(options.toBuilder().build().toString()).isEqualTo(options.toString())
                .contains("batchAcknowledgements=true", "ackBatchSize=5");
    }
}
