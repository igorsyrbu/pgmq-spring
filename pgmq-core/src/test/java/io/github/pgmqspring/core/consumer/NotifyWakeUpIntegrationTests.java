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

import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.sql.DataSource;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import io.github.pgmqspring.core.PgmqContainerSupport;
import io.github.pgmqspring.core.UnsupportedPgmqFeatureException;
import io.github.pgmqspring.core.client.PgmqOperations;
import io.github.pgmqspring.core.client.PgmqTemplate;
import io.github.pgmqspring.core.convert.JacksonPayloadConverter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * {@link WakeUp#NOTIFY}: the container wakes on PGMQ's insert notifications, so its regular poll
 * can be a slow fallback. Every container here waits 20s between polls, so anything picked up
 * within a few seconds was picked up because something woke it.
 */
class NotifyWakeUpIntegrationTests {

    private static final Duration FALLBACK_POLL = Duration.ofSeconds(20);

    private static PgmqTemplate pgmq;

    private static JdbcTemplate jdbc;

    private final ConcurrentLinkedQueue<PgmqMessageListenerContainer<?>> containers = new ConcurrentLinkedQueue<>();

    @BeforeAll
    static void setUp() {
        DataSource dataSource = PgmqContainerSupport.dataSource();
        pgmq = new PgmqTemplate(dataSource, new JacksonPayloadConverter());
        jdbc = new JdbcTemplate(dataSource);
    }

    @AfterEach
    void stopContainers() {
        PgmqMessageListenerContainer<?> container;
        while ((container = this.containers.poll()) != null) {
            container.stop();
        }
    }

    private static boolean notifySupported() {
        return pgmq.capabilities().insertNotify();
    }

    private static String newQueue(String prefix) {
        String queue = PgmqContainerSupport.uniqueQueueName(prefix);
        pgmq.createQueue(queue);
        return queue;
    }

    private static ConsumerOptions.Builder notifyOptions() {
        return ConsumerOptions.builder()
                .wakeUp(WakeUp.NOTIFY)
                .pollDelay(FALLBACK_POLL)
                .maxPollDelay(FALLBACK_POLL)
                .shutdownTimeout(Duration.ofSeconds(5));
    }

    private <T> PgmqMessageListenerContainer<T> start(PgmqMessageListenerContainer<T> container) {
        this.containers.add(container);
        container.start();
        return container;
    }

    /** Backend pids of connections listening for inserts into {@code queue}. */
    private static List<Integer> listeners(String queue) {
        return jdbc.queryForList("select pid from pg_stat_activity where query = ?", Integer.class,
                "listen \"" + PgmqOperations.notifyInsertChannel(queue) + "\"");
    }

    private PgmqMessageListenerContainer<String> recording(String queue, List<Long> handledAtMillis) {
        return start(PgmqMessageListenerContainer.builder(pgmq, queue, String.class)
                .options(notifyOptions().build())
                .handler((message) -> handledAtMillis.add(System.currentTimeMillis()))
                .build());
    }

    @Test
    void enablesInspectsAndDisablesInsertNotifications() {
        assumeTrue(notifySupported(), "insert notifications need PGMQ 1.10.0");
        String queue = newQueue("notify_toggle");

        assertThat(pgmq.notifyInsertThrottle(queue)).isNull();
        pgmq.enableNotifyInsert(queue);
        assertThat(pgmq.notifyInsertThrottle(queue)).isEqualTo(Duration.ofMillis(250));
        pgmq.enableNotifyInsert(queue, Duration.ofMillis(40));
        assertThat(pgmq.notifyInsertThrottle(queue)).isEqualTo(Duration.ofMillis(40));
        pgmq.disableNotifyInsert(queue);
        assertThat(pgmq.notifyInsertThrottle(queue)).isNull();
        // A mixed-case name must notify too: PGMQ's trigger looks its throttle up lower-cased.
        pgmq.enableNotifyInsert(queue.toUpperCase(java.util.Locale.ROOT));
        assertThat(pgmq.notifyInsertThrottle(queue)).isNotNull();
    }

    @Test
    void aNotifiedMessageIsConsumedWithoutWaitingForTheFallbackPoll() {
        assumeTrue(notifySupported(), "insert notifications need PGMQ 1.10.0");
        String queue = newQueue("notify_pickup");
        pgmq.enableNotifyInsert(queue);
        List<Long> handledAtMillis = new CopyOnWriteArrayList<>();
        recording(queue, handledAtMillis);
        await().atMost(Duration.ofSeconds(10)).until(() -> listeners(queue).size() == 1);

        long sentAt = System.currentTimeMillis();
        pgmq.send(queue, "wake up");

        await().atMost(Duration.ofSeconds(10)).until(() -> handledAtMillis.size() == 1);
        assertThat(handledAtMillis.get(0) - sentAt).isLessThan(5000L);
    }

    @Test
    void theListenerReconnectsAfterItsConnectionIsKilled() {
        assumeTrue(notifySupported(), "insert notifications need PGMQ 1.10.0");
        String queue = newQueue("notify_reconnect");
        pgmq.enableNotifyInsert(queue);
        List<Long> handledAtMillis = new CopyOnWriteArrayList<>();
        recording(queue, handledAtMillis);
        await().atMost(Duration.ofSeconds(10)).until(() -> listeners(queue).size() == 1);
        int killed = listeners(queue).get(0);

        jdbc.queryForObject("select pg_terminate_backend(?)", Boolean.class, killed);

        await().atMost(Duration.ofSeconds(15))
                .until(() -> listeners(queue).size() == 1 && listeners(queue).get(0) != killed);
        long sentAt = System.currentTimeMillis();
        pgmq.send(queue, "after the reconnect");
        await().atMost(Duration.ofSeconds(10)).until(() -> handledAtMillis.size() == 1);
        assertThat(handledAtMillis.get(0) - sentAt).isLessThan(5000L);
    }

    @Test
    void aRetryWakesTheContainerWhenItIsDue() {
        assumeTrue(notifySupported(), "insert notifications need PGMQ 1.10.0");
        String queue = newQueue("notify_retry");
        pgmq.enableNotifyInsert(queue);
        List<Long> deliveredAtMillis = new CopyOnWriteArrayList<>();

        start(PgmqMessageListenerContainer.builder(pgmq, queue, String.class)
                .options(notifyOptions().retryDelay(Duration.ofSeconds(1)).build())
                .handler((message) -> {
                    deliveredAtMillis.add(System.currentTimeMillis());
                    if (message.readCount() == 1) {
                        throw new IllegalStateException("fails once");
                    }
                })
                .build());
        await().atMost(Duration.ofSeconds(10)).until(() -> listeners(queue).size() == 1);
        pgmq.send(queue, "retried");

        // Making a message visible again sends no notification; the container's own wake-up must.
        await().atMost(Duration.ofSeconds(15)).until(() -> deliveredAtMillis.size() == 2);
        assertThat(deliveredAtMillis.get(1) - deliveredAtMillis.get(0)).isLessThan(5000L);
    }

    @Test
    void startFailsWhenTheQueueDoesNotNotify() {
        assumeTrue(notifySupported(), "insert notifications need PGMQ 1.10.0");
        String queue = newQueue("notify_disabled");

        PgmqMessageListenerContainer<String> container = PgmqMessageListenerContainer
                .builder(pgmq, queue, String.class)
                .options(notifyOptions().build())
                .handler((message) -> { })
                .build();

        assertThatIllegalStateException().isThrownBy(container::start)
                .withMessageContaining("does not notify on inserts")
                .withMessageContaining("pgmq.queues[].notify-on-insert");
        assertThat(container.isRunning()).isFalse();
    }

    @Test
    void startFailsOnPgmqWithoutInsertNotifications() {
        assumeFalse(notifySupported(), "only PGMQ before 1.10.0 lacks insert notifications");
        String queue = newQueue("notify_unsupported");

        PgmqMessageListenerContainer<String> container = PgmqMessageListenerContainer
                .builder(pgmq, queue, String.class)
                .options(notifyOptions().build())
                .handler((message) -> { })
                .build();

        assertThatExceptionOfType(UnsupportedPgmqFeatureException.class).isThrownBy(container::start)
                .withMessageContaining("1.10.0");
        assertThatExceptionOfType(UnsupportedPgmqFeatureException.class)
                .isThrownBy(() -> pgmq.enableNotifyInsert(queue));
        assertThat(pgmq.notifyInsertThrottle(queue)).isNull();
    }

    @Test
    void stoppingDoesNotWaitOutAPollDelay() {
        String queue = newQueue("prompt_stop");
        PgmqMessageListenerContainer<String> container = start(PgmqMessageListenerContainer
                .builder(pgmq, queue, String.class)
                .options(ConsumerOptions.builder().pollDelay(FALLBACK_POLL).maxPollDelay(FALLBACK_POLL).build())
                .handler((message) -> { })
                .build());
        await().pollDelay(Duration.ofMillis(500)).until(() -> true);

        long stoppingAt = System.currentTimeMillis();
        container.stop();

        assertThat(System.currentTimeMillis() - stoppingAt).isLessThan(3000L);
    }

    @Test
    void rejectsNotifyWhereItCannotWork() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> ConsumerOptions.builder().wakeUp(WakeUp.NOTIFY).longPoll(Duration.ofSeconds(5)).build())
                .withMessageContaining("wakeUp=NOTIFY cannot be combined with longPoll");
        PgmqOperations custom = (PgmqOperations) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[] {PgmqOperations.class}, (proxy, method, args) -> method.invoke(pgmq, args));
        assertThatIllegalArgumentException()
                .isThrownBy(() -> PgmqMessageListenerContainer.builder(custom, "orders", String.class)
                        .options(ConsumerOptions.builder().wakeUp(WakeUp.NOTIFY).build())
                        .handler((message) -> { })
                        .build())
                .withMessageContaining("notificationDataSource");
        ConsumerOptions options = ConsumerOptions.builder().wakeUp(WakeUp.NOTIFY).build();
        assertThat(options.toBuilder().build().toString()).isEqualTo(options.toString()).contains("wakeUp=NOTIFY");
        assertThat(ConsumerOptions.defaults().getWakeUp()).isEqualTo(WakeUp.POLL);
    }
}
