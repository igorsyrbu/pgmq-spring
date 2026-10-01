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

package io.github.pgmqspring.samples.batch;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.TestPropertySource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import io.github.pgmqspring.core.PgmqContainerSupport;
import io.github.pgmqspring.core.client.PgmqTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration tests for the batch quickstart: batch sending, batch consumption, and the two rules
 * a batch handler has to follow.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@TestPropertySource(properties = {"spring.sql.init.mode=always", "sample.demo.enabled=false"})
class BatchQuickstartIntegrationTests {

    @Autowired
    private ReadingService readingService;

    @Autowired
    private ReadingBatchHandler handler;

    @Autowired
    private JdbcClient jdbc;

    @Autowired
    private PgmqTemplate pgmq;

    @Autowired
    private MeterRegistry registry;

    @Autowired
    private PlatformTransactionManager transactionManager;

    private static List<Reading> readings(String prefix, int count) {
        List<Reading> readings = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            readings.add(new Reading(prefix + "-" + i, "sensor-" + (i % 3), 20 + i * 0.5));
        }
        return readings;
    }

    private int count(String table, String prefix) {
        return this.jdbc.sql("select count(*) from " + table + " where id like ?")
                .param(prefix + "-%")
                .query(Integer.class)
                .single();
    }

    private double sendStatements() {
        var timer = this.registry.find("pgmq.send.duration").tag("queue", ReadingService.QUEUE).timer();
        return timer != null ? timer.count() : 0;
    }

    @Test
    void readingsAreSentInOneStatementAndConsumedInBatches() {
        double statementsBefore = sendStatements();

        List<Long> ids = this.readingService.ingest("ingest-bulk", readings("bulk", 200));

        assertThat(ids).hasSize(200).isSorted();
        assertThat(sendStatements() - statementsBefore).as("200 messages, one statement").isEqualTo(1);
        await().atMost(Duration.ofSeconds(30)).until(() -> count("readings", "bulk") == 200);
        assertThat(this.handler.largestBatch()).as("the handler received batches, not single messages")
                .isGreaterThan(1)
                .isLessThanOrEqualTo(50);
    }

    @Test
    void anInvalidReadingIsRejectedWithoutFailingItsBatchMates() {
        List<Reading> readings = readings("mixed", 10);
        readings.set(3, new Reading("mixed-bad", "sensor-0", 9999));

        this.readingService.ingest("ingest-mixed", readings);

        await().atMost(Duration.ofSeconds(30)).until(() -> count("readings", "mixed") == 9);
        assertThat(count("rejected_readings", "mixed")).isEqualTo(1);
        assertThat(this.pgmq.metrics("readings_dlq").queueLength()).isZero();
    }

    @Test
    void aTransientFailureRetriesTheWholeBatchAndStoresEveryReadingOnce() {
        List<Reading> readings = readings("retry", 20);
        readings.set(7, new Reading("retry-flaky", ReadingBatchHandler.FLAKY_DEVICE, 1.0));

        this.readingService.ingest("ingest-retry", readings);

        // The first delivery of the flaky reading's batch throws, so the batch rolls back and every
        // message in it is redelivered; the idempotent insert stores each reading exactly once.
        await().atMost(Duration.ofSeconds(30)).until(() -> count("readings", "retry") == 20);
        assertThat(this.jdbc.sql("select count(distinct id) from readings where id like 'retry-%'")
                .query(Integer.class).single()).isEqualTo(20);
        assertThat(this.pgmq.metrics("readings_dlq").queueLength()).isZero();
    }

    @Test
    void aRollbackAfterTheSendDiscardsTheWholeBatch() {
        // The rollback happens after sendBatch has run, so this proves the send joined the
        // transaction - not merely that it was never reached.
        new TransactionTemplate(this.transactionManager).executeWithoutResult((status) -> {
            List<Long> ids = this.readingService.ingest("ingest-rollback", readings("rollback", 5));
            assertThat(ids).hasSize(5);
            status.setRollbackOnly();
        });

        assertThat(this.jdbc.sql("select count(*) from ingestions where id = 'ingest-rollback'")
                .query(Integer.class).single()).isZero();
        await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(5)).until(() -> this.jdbc
                .sql("select count(*) from pgmq.q_readings where message->>'id' like 'rollback-%'")
                .query(Integer.class).single() == 0 && count("readings", "rollback") == 0);
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class ContainerConfiguration {

        // The pool is shared by every test in the JVM, so the context must not close it.
        @Bean(destroyMethod = "")
        @Primary
        DataSource dataSource() {
            return PgmqContainerSupport.dataSource();
        }
    }
}
