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
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.github.pgmqspring.core.PgmqContainerSupport;
import io.github.pgmqspring.core.PgmqMessage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 * Integration tests for grouped (FIFO-per-key) reads.
 *
 * <p>The central guarantee - only one message per group in flight at a time, across every reader -
 * is what makes ordered per-key processing possible on a competing-consumer queue, so it is
 * asserted directly rather than inferred.
 *
 * <p>Requires PGMQ 1.10.0+; skipped against older images.
 */
class GroupedReadIntegrationTests {

    private static PgmqTemplate pgmq;

    @BeforeAll
    static void setUp() {
        pgmq = PgmqContainerSupport.template();
        Assumptions.assumeTrue(pgmq.capabilities().groupedReads(),
                "grouped reads require PGMQ 1.10.0+; installed: " + pgmq.capabilities().version());
    }

    private static String newFifoQueue(String prefix) {
        String queue = PgmqContainerSupport.newQueue(prefix);
        pgmq.createFifoIndex(queue);
        return queue;
    }

    private static List<String> groupsOf(List<PgmqMessage<String>> messages) {
        return messages.stream().map(PgmqMessage::groupKey).toList();
    }

    @Test
    void withholdsAWholeGroupWhileOneOfItsMessagesIsInFlight() {
        String queue = newFifoQueue("grp_exclusive");
        pgmq.send(queue, Map.of("n", 1), SendOptions.none().group("user-42"));
        pgmq.send(queue, Map.of("n", 2), SendOptions.none().group("user-42"));
        pgmq.send(queue, Map.of("n", 3), SendOptions.none().group("user-42"));

        // Consumer A claims the group's oldest message.
        List<PgmqMessage<String>> first = pgmq.readGrouped(
                queue, ReadOptions.batch(1).visibilityTimeout(Duration.ofSeconds(30)));
        assertThat(first).singleElement()
                .satisfies((message) -> assertThat(message.rawPayload()).contains("\"n\": 1"));

        // Consumer B polls: messages 2 and 3 are both past their visibility time, but the group is
        // withheld entirely until message 1 is resolved.
        assertThat(pgmq.readGrouped(queue, ReadOptions.batch(10)))
                .as("no second message from a group with one in flight")
                .isEmpty();

        // Acknowledging the head releases the next message, in order.
        pgmq.delete(queue, first.get(0).id());
        assertThat(pgmq.readGrouped(queue, ReadOptions.batch(1)))
                .singleElement()
                .satisfies((message) -> assertThat(message.rawPayload()).contains("\"n\": 2"));
    }

    @Test
    void greedyStrategyDrainsOneGroupBeforeTheNext() {
        String queue = seedTwoGroups("grp_greedy");

        List<PgmqMessage<String>> read = pgmq.readGrouped(
                queue, ReadOptions.batch(10).groupStrategy(GroupReadStrategy.GREEDY));

        assertThat(groupsOf(read)).containsExactly("A", "A", "A", "B", "B");
    }

    @Test
    void headStrategyReturnsAtMostOneMessagePerGroup() {
        String queue = seedTwoGroups("grp_head");

        List<PgmqMessage<String>> read = pgmq.readGrouped(
                queue, ReadOptions.batch(10).groupStrategy(GroupReadStrategy.HEAD));

        // One per group, however large the requested batch.
        assertThat(groupsOf(read)).containsExactly("A", "B");
    }

    @Test
    void roundRobinStrategySpreadsTheBatchAcrossGroups() {
        String queue = seedTwoGroups("grp_rr");

        List<PgmqMessage<String>> read = pgmq.readGrouped(
                queue, ReadOptions.batch(10).groupStrategy(GroupReadStrategy.ROUND_ROBIN));

        // Every group gets one before any gets a second.
        assertThat(groupsOf(read)).containsExactly("A", "B", "A", "B", "A");
    }

    @Test
    void longPollingWorksForEveryStrategy() {
        for (GroupReadStrategy strategy : GroupReadStrategy.values()) {
            String queue = newFifoQueue("grp_poll");
            pgmq.send(queue, "waiting", SendOptions.none().group("g"));

            List<PgmqMessage<String>> read = pgmq.readGrouped(queue, ReadOptions.batch(1)
                    .groupStrategy(strategy)
                    .longPoll(Duration.ofSeconds(5)));

            assertThat(read).as("strategy %s", strategy).hasSize(1);
        }
    }

    @Test
    void longPollingReturnsEmptyWhenTheWindowExpires() {
        String queue = newFifoQueue("grp_poll_empty");

        long startedAt = System.nanoTime();
        List<PgmqMessage<String>> read = pgmq.readGrouped(
                queue, ReadOptions.batch(1).longPoll(Duration.ofSeconds(1)));
        Duration elapsed = Duration.ofNanos(System.nanoTime() - startedAt);

        assertThat(read).isEmpty();
        assertThat(elapsed).isGreaterThanOrEqualTo(Duration.ofMillis(900));
    }

    @Test
    void differentGroupsDoNotBlockEachOther() {
        String queue = newFifoQueue("grp_independent");
        pgmq.send(queue, "a", SendOptions.none().group("user-a"));
        pgmq.send(queue, "b", SendOptions.none().group("user-b"));

        List<PgmqMessage<String>> read = pgmq.readGrouped(queue, ReadOptions.batch(10));

        assertThat(groupsOf(read)).containsExactlyInAnyOrder("user-a", "user-b");
    }

    @Test
    void messagesSentWithoutAGroupShareTheImplicitDefaultGroup() {
        String queue = newFifoQueue("grp_default");
        pgmq.send(queue, "first");
        pgmq.send(queue, "second");

        List<PgmqMessage<String>> first = pgmq.readGrouped(queue, ReadOptions.batch(1));
        assertThat(first).singleElement().satisfies((message) -> {
            assertThat(message.groupKey()).isNull();
            assertThat(message.rawPayload()).contains("first");
        });

        // The implicit group is serialized exactly like an explicit one.
        assertThat(pgmq.readGrouped(queue, ReadOptions.batch(10))).isEmpty();
    }

    @Test
    void groupOrderIsPreservedAcrossTheWholeQueue() {
        String queue = newFifoQueue("grp_order");
        for (int i = 0; i < 8; i++) {
            pgmq.send(queue, Map.of("seq", i), SendOptions.none().group("ordered"));
        }

        for (int expected = 0; expected < 8; expected++) {
            PgmqMessage<String> message = pgmq.readGrouped(queue, ReadOptions.batch(1)).get(0);
            assertThat(message.rawPayload()).contains("\"seq\": " + expected);
            pgmq.delete(queue, message.id());
        }
    }

    @Test
    void conditionalFilterIsRejectedBecausePgmqHasNoSuchOverload() {
        String queue = newFifoQueue("grp_conditional");

        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> pgmq.readGrouped(
                        queue, ReadOptions.batch(1).conditional(Map.of("k", "v"))))
                .withMessageContaining("conditional");
    }

    @Test
    void groupHelpersUseTheExactHeaderPgmqReads() {
        String queue = newFifoQueue("grp_header");
        pgmq.send(queue, "payload", SendOptions.none().group("checked"));

        PgmqMessage<String> message = pgmq.read(queue, ReadOptions.defaults()).get(0);

        assertThat(message.header(FifoGroups.GROUP_HEADER)).isEqualTo("checked");
        assertThat(message.groupKey()).isEqualTo("checked");
    }

    @Test
    void setVisibleAtDefersAMessageToAnAbsoluteInstant() {
        String queue = newFifoQueue("grp_visible_at");
        long id = pgmq.send(queue, "deferred");
        pgmq.read(queue, ReadOptions.defaults());

        pgmq.setVisibleAt(queue, id, Instant.now().plus(Duration.ofHours(2)));

        assertThat(pgmq.read(queue, ReadOptions.batch(10))).isEmpty();
        assertThat(pgmq.metrics(queue).queueLength()).isEqualTo(1);
    }

    @Test
    void setVisibleAtInThePastMakesAMessageImmediatelyAvailable() {
        String queue = newFifoQueue("grp_visible_now");
        long id = pgmq.send(queue, "revived");
        pgmq.read(queue, ReadOptions.defaults().visibilityTimeout(Duration.ofMinutes(10)));
        assertThat(pgmq.read(queue, ReadOptions.batch(10))).isEmpty();

        pgmq.setVisibleAt(queue, id, Instant.now().minusSeconds(1));

        assertThat(pgmq.read(queue, ReadOptions.batch(10))).hasSize(1);
    }

    @Test
    void setVisibleAtAcceptsABatch() {
        String queue = newFifoQueue("grp_visible_batch");
        List<Long> ids = pgmq.sendBatch(queue, List.of("a", "b", "c"));
        pgmq.read(queue, ReadOptions.batch(10));

        pgmq.setVisibleAt(queue, ids, Instant.now().minusSeconds(1));

        assertThat(pgmq.read(queue, ReadOptions.batch(10))).hasSize(3);

        // An empty batch is a no-op rather than an error.
        pgmq.setVisibleAt(queue, List.of(), Instant.now());
    }

    private String seedTwoGroups(String prefix) {
        String queue = newFifoQueue(prefix);
        for (int n = 1; n <= 3; n++) {
            pgmq.send(queue, Map.of("n", n), SendOptions.none().group("A"));
        }
        for (int n = 4; n <= 5; n++) {
            pgmq.send(queue, Map.of("n", n), SendOptions.none().group("B"));
        }
        return queue;
    }
}
