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
import java.util.concurrent.ConcurrentLinkedQueue;

import javax.sql.DataSource;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionTimedOutException;

import io.github.pgmqspring.core.PgmqContainerSupport;
import io.github.pgmqspring.core.PgmqMessage;
import io.github.pgmqspring.core.client.PgmqTemplate;
import io.github.pgmqspring.core.convert.JacksonPayloadConverter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.awaitility.Awaitility.await;

/** {@link ConsumerOptions.Builder#transactionTimeout(Duration)}. */
class TransactionTimeoutIntegrationTests {

    private static PgmqTemplate pgmq;

    private static PlatformTransactionManager transactionManager;

    private static JdbcTemplate jdbc;

    private PgmqMessageListenerContainer<String> container;

    @BeforeAll
    static void setUp() {
        DataSource dataSource = PgmqContainerSupport.dataSource();
        pgmq = new PgmqTemplate(dataSource, new JacksonPayloadConverter());
        transactionManager = new DataSourceTransactionManager(dataSource);
        jdbc = new JdbcTemplate(dataSource);
    }

    @AfterEach
    void stopContainer() {
        if (this.container != null) {
            this.container.stop();
        }
    }

    @Test
    void aHandlerThatOverrunsItsTransactionRollsBackAndIsRetried() {
        String queue = PgmqContainerSupport.uniqueQueueName("tx_timeout");
        pgmq.createQueue(queue);
        String table = "public." + queue + "_writes";
        jdbc.execute("create table " + table + " (read_count int not null)");
        ConcurrentLinkedQueue<Throwable> failures = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<Integer> succeeded = new ConcurrentLinkedQueue<>();

        this.container = PgmqMessageListenerContainer.builder(pgmq, queue, String.class)
                .options(ConsumerOptions.builder()
                        .transactional(true)
                        .transactionTimeout(Duration.ofSeconds(1))
                        .retryDelay(Duration.ZERO)
                        .visibilityTimeout(Duration.ofSeconds(3))
                        .pollDelay(Duration.ofMillis(50))
                        .maxPollDelay(Duration.ofMillis(100))
                        .build())
                .transactionManager(transactionManager)
                .listener(new ConsumerListener() {
                    @Override
                    public void onSuccess(String polledQueue, PgmqMessage<?> message, Duration took) {
                        succeeded.add(message.readCount());
                    }

                    @Override
                    public void onFailure(String polledQueue, PgmqMessage<?> message, Duration took, Throwable error) {
                        failures.add(error);
                    }
                })
                .handler((message) -> {
                    jdbc.update("insert into " + table + " values (?)", message.readCount());
                    if (message.readCount() == 1) {
                        // Past the 1s deadline between two statements: the next one must fail.
                        Thread.sleep(2500);
                    }
                    jdbc.update("insert into " + table + " values (?)", message.readCount());
                })
                .build();
        this.container.start();
        pgmq.send(queue, "slow the first time");

        await().atMost(Duration.ofSeconds(30)).until(() -> succeeded.contains(2));
        assertThat(failures).singleElement().isInstanceOf(TransactionTimedOutException.class);
        // The first delivery's write rolled back; only the retry's two rows remain.
        assertThat(jdbc.queryForList("select read_count from " + table, Integer.class)).isEqualTo(List.of(2, 2));
        await().atMost(Duration.ofSeconds(10)).until(() -> pgmq.metrics(queue).queueLength() == 0);
    }

    @Test
    void validatesTheTimeout() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> ConsumerOptions.builder().transactionTimeout(Duration.ofSeconds(30)).build())
                .withMessageContaining("transactionTimeout requires transactional=true");
        assertThatIllegalArgumentException()
                .isThrownBy(() -> ConsumerOptions.builder()
                        .transactional(true).transactionTimeout(Duration.ZERO).build())
                .withMessageContaining("transactionTimeout must be positive");
        ConsumerOptions options = ConsumerOptions.builder()
                .transactional(true).transactionTimeout(Duration.ofSeconds(30)).build();
        assertThat(options.toBuilder().build().toString()).isEqualTo(options.toString())
                .contains("transactionTimeout=PT30S");
        assertThat(ConsumerOptions.defaults().getTransactionTimeout()).isNull();
    }
}
