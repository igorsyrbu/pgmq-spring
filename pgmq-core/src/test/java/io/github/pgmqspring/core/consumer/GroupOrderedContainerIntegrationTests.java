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
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import io.github.pgmqspring.core.ListenerContainers;
import io.github.pgmqspring.core.PgmqContainerSupport;
import io.github.pgmqspring.core.client.GroupReadStrategy;
import io.github.pgmqspring.core.client.PgmqTemplate;
import io.github.pgmqspring.core.client.SendOptions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The property group-ordered mode exists for: with several poller threads racing on one queue,
 * messages for the same key are never handled concurrently and arrive in order.
 *
 * <p>Requires PGMQ 1.10.0+; skipped against older images.
 */
class GroupOrderedContainerIntegrationTests {

    private static PgmqTemplate pgmq;

    private static PlatformTransactionManager transactionManager;

    @RegisterExtension
    final ListenerContainers containers = new ListenerContainers();

    @BeforeAll
    static void setUp() {
        pgmq = PgmqContainerSupport.template();
        transactionManager = new DataSourceTransactionManager(PgmqContainerSupport.dataSource());
        Assumptions.assumeTrue(pgmq.capabilities().groupedReads(),
                "grouped reads require PGMQ 1.10.0+; installed: " + pgmq.capabilities().version());
    }

    private static String newFifoQueue(String prefix) {
        String queue = PgmqContainerSupport.newQueue(prefix);
        pgmq.createFifoIndex(queue);
        return queue;
    }

    @Test
    void neverHandlesOneKeyConcurrentlyAndKeepsItsOrder() throws Exception {
        String queue = newFifoQueue("grp_container");
        int users = 6;
        int perUser = 8;
        int total = users * perUser;

        for (int u = 0; u < users; u++) {
            String userId = "user-" + u;
            for (int seq = 0; seq < perUser; seq++) {
                pgmq.send(queue, Map.of("userId", userId, "seq", seq), SendOptions.none().group(userId));
            }
        }

        ConcurrentHashMap<String, Boolean> inFlight = new ConcurrentHashMap<>();
        ConcurrentHashMap<String, Integer> lastSeq = new ConcurrentHashMap<>();
        ConcurrentLinkedQueue<String> overlaps = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<String> outOfOrder = new ConcurrentLinkedQueue<>();
        CountDownLatch done = new CountDownLatch(total);

        this.containers.start(PgmqMessageListenerContainer.builder(pgmq, queue, Map.class)
                .options(ConsumerOptions.builder()
                        .concurrency(5)
                        .batchSize(1)
                        .groupOrdered(true)
                        .pollDelay(Duration.ofMillis(20))
                        .maxPollDelay(Duration.ofMillis(100))
                        .visibilityTimeout(Duration.ofSeconds(30))
                        .build())
                .handler((message) -> {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> payload = message.payload();
                    String userId = (String) payload.get("userId");
                    int seq = ((Number) payload.get("seq")).intValue();

                    if (Boolean.TRUE.equals(inFlight.putIfAbsent(userId, Boolean.TRUE))) {
                        overlaps.add(userId + " seq=" + seq);
                    }
                    Thread.sleep(15);
                    Integer previous = lastSeq.put(userId, seq);
                    if (previous != null && seq != previous + 1) {
                        outOfOrder.add(userId + ": " + previous + " -> " + seq);
                    }
                    inFlight.remove(userId);
                    done.countDown();
                })
                .build());

        assertThat(done.await(90, TimeUnit.SECONDS)).as("all %d handled", total).isTrue();
        assertThat(overlaps).as("no key handled by two threads at once").isEmpty();
        assertThat(outOfOrder).as("each key handled in send order").isEmpty();
    }

    @Test
    void roundRobinKeepsABusyKeyFromStarvingOthers() {
        String queue = newFifoQueue("grp_fair");
        for (int i = 0; i < 30; i++) {
            pgmq.send(queue, Map.of("i", i), SendOptions.none().group("noisy"));
        }
        pgmq.send(queue, Map.of("i", -1), SendOptions.none().group("quiet"));

        ConcurrentLinkedQueue<String> order = new ConcurrentLinkedQueue<>();
        this.containers.start(PgmqMessageListenerContainer.builder(pgmq, queue, Map.class)
                .options(ConsumerOptions.builder()
                        .batchSize(5)
                        .groupOrdered(true)
                        .groupStrategy(GroupReadStrategy.ROUND_ROBIN)
                        .pollDelay(Duration.ofMillis(20))
                        .build())
                .handler((message) -> order.add(message.groupKey()))
                .build());

        // The quiet key is served early rather than after all 30 noisy messages.
        await().atMost(Duration.ofSeconds(30)).until(() -> order.contains("quiet"));
        assertThat(order.stream().takeWhile((g) -> !"quiet".equals(g)).count())
                .as("quiet key not starved behind the whole noisy backlog")
                .isLessThan(10);
    }

    @Test
    void manualRetryAtDefersToAnAbsoluteInstant() {
        String queue = newFifoQueue("grp_retry_at");
        AtomicInteger deliveries = new AtomicInteger();

        this.containers.start(PgmqMessageListenerContainer.builder(pgmq, queue, String.class)
                .options(ConsumerOptions.builder()
                        .acknowledgeMode(AcknowledgeMode.MANUAL)
                        .pollDelay(Duration.ofMillis(20))
                        .maxAttempts(50)
                        .build())
                .acknowledgingHandler((message, ack) -> {
                    deliveries.incrementAndGet();
                    ack.retryAt(Instant.now().plus(Duration.ofHours(1)));
                })
                .build());

        pgmq.send(queue, "deferred");

        await().atMost(Duration.ofSeconds(20)).until(() -> deliveries.get() == 1);
        // Deferred an hour out, so it must not come back during the test.
        await().pollDelay(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(deliveries.get()).isEqualTo(1));
        assertThat(pgmq.metrics(queue).queueLength()).isEqualTo(1);
    }

    @Test
    void groupedModeWorksWithTransactionalProcessingAndLongPolling() {
        String queue = newFifoQueue("grp_tx_poll");
        CountDownLatch handled = new CountDownLatch(3);

        this.containers.start(PgmqMessageListenerContainer.builder(pgmq, queue, String.class)
                .options(ConsumerOptions.builder()
                        .groupOrdered(true)
                        .groupStrategy(GroupReadStrategy.HEAD)
                        .longPoll(Duration.ofSeconds(2))
                        .transactional(true)
                        .batchSize(1)
                        .build())
                .transactionManager(transactionManager)
                .handler((message) -> handled.countDown())
                .build());

        for (int i = 0; i < 3; i++) {
            pgmq.send(queue, "m" + i, SendOptions.none().group("g" + i));
        }

        await().atMost(Duration.ofSeconds(30)).until(() -> handled.getCount() == 0);
        await().atMost(Duration.ofSeconds(20)).until(() -> pgmq.metrics(queue).queueLength() == 0);
    }

    @Test
    void groupStrategyDefaultsToHeadSoBatchesNeverMixOneGroup() {
        assertThat(ConsumerOptions.defaults().getGroupStrategy()).isEqualTo(GroupReadStrategy.HEAD);
        assertThat(ConsumerOptions.defaults().isGroupOrdered()).isFalse();
    }
}
