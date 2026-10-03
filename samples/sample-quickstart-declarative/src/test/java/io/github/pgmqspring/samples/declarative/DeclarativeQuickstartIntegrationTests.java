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

package io.github.pgmqspring.samples.declarative;

import java.time.Duration;

import javax.sql.DataSource;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.TestPropertySource;

import io.github.pgmqspring.core.PgmqContainerSupport;
import io.github.pgmqspring.core.PgmqMessage;
import io.github.pgmqspring.core.client.PgmqTemplate;
import io.github.pgmqspring.core.client.ReadOptions;
import io.github.pgmqspring.core.consumer.ConsumerOptions;
import io.github.pgmqspring.core.consumer.DeadLetterHeaders;
import io.github.pgmqspring.core.consumer.FailureAction;
import io.github.pgmqspring.core.consumer.PgmqMessageListenerContainer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration tests for the declarative sample: containers that exist only in configuration
 * consume, inherit the defaults, and are instrumented.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@TestPropertySource(properties = {"spring.sql.init.mode=always", "sample.demo.enabled=false"})
class DeclarativeQuickstartIntegrationTests {

    @Autowired
    private InvoiceService invoiceService;

    @Autowired
    private AuditBatchHandler auditHandler;

    @Autowired
    private JdbcClient jdbc;

    @Autowired
    private PgmqTemplate pgmq;

    @Autowired
    private ApplicationContext applicationContext;

    private int count(String sql, Object parameter) {
        return this.jdbc.sql(sql).param(parameter).query(Integer.class).single();
    }

    private static double acknowledged(MeterRegistry registry) {
        Counter counter = registry.find("pgmq.messages.acknowledged").tag("queue", InvoiceService.INVOICES).counter();
        return counter != null ? counter.count() : 0;
    }

    private PgmqMessageListenerContainer<?> container(String name) {
        return this.applicationContext.getBean("pgmqConsumer-" + name, PgmqMessageListenerContainer.class);
    }

    @Test
    void theDeclaredConsumersExistAsConfigured() {
        ConsumerOptions invoices = container("invoices").getOptions();
        assertThat(container("invoices").getQueue()).isEqualTo(InvoiceService.INVOICES);
        assertThat(container("invoices").isRunning()).isTrue();
        assertThat(invoices.getConcurrency()).isEqualTo(2);
        assertThat(invoices.isTransactional()).isTrue();
        assertThat(invoices.getFailureAction()).isEqualTo(FailureAction.DEAD_LETTER);
        // Inherited from pgmq.consumer.
        assertThat(invoices.getMaxAttempts()).isEqualTo(3);
        assertThat(invoices.getRetryMultiplier()).isEqualTo(2.0);

        ConsumerOptions audit = container("audit").getOptions();
        assertThat(container("audit").getQueue()).isEqualTo(InvoiceService.AUDIT);
        assertThat(audit.getBatchSize()).isEqualTo(20);
        assertThat(audit.getMaxAttempts()).isEqualTo(5);
        assertThat(audit.getRetryMultiplier()).isEqualTo(2.0);
    }

    @Test
    void anIssuedInvoiceIsEmailedAndAudited() {
        this.invoiceService.issue("inv-ok", "alice", 2500);

        await().atMost(Duration.ofSeconds(30)).until(() ->
                count("select count(*) from invoice_emails where invoice_id = ?", "inv-ok") == 1
                        && count("select count(*) from audit_log where subject = ?", "inv-ok") == 1);
    }

    @Test
    void theAuditTrailIsConsumedInBatches() {
        PgmqMessageListenerContainer<?> audit = container("audit");
        audit.pause();
        try {
            for (int i = 0; i < 30; i++) {
                this.invoiceService.issue("inv-bulk-" + i, "customer-" + i, 100);
            }
        }
        finally {
            audit.resume();
        }

        await().atMost(Duration.ofSeconds(45)).until(() ->
                count("select count(*) from audit_log where subject like ?", "inv-bulk-%") == 30);
        assertThat(this.auditHandler.largestBatch()).isGreaterThan(1).isLessThanOrEqualTo(20);
    }

    @Test
    void anInvoiceForNothingIsDeadLetteredOnItsFirstFailure() {
        this.invoiceService.issue("inv-empty", "mallory", 0);

        await().atMost(Duration.ofSeconds(30)).until(() -> this.pgmq.metrics("invoices_dlq").queueLength() >= 1);
        assertThat(this.pgmq.read("invoices_dlq", ReadOptions.batch(10)))
                .filteredOn((PgmqMessage<String> message) -> message.rawPayload().contains("inv-empty"))
                .singleElement()
                .satisfies((message) -> assertThat(message.header(DeadLetterHeaders.READ_COUNT)).isEqualTo(1));
        assertThat(count("select count(*) from invoice_emails where invoice_id = ?", "inv-empty")).isZero();
    }

    @Test
    void declaredConsumersAreInstrumentedLikeAnyOther() {
        MeterRegistry registry = this.applicationContext.getBean(MeterRegistry.class);
        double acknowledgedBefore = acknowledged(registry);
        this.invoiceService.issue("inv-metered", "alice", 1);

        await().atMost(Duration.ofSeconds(30)).until(() -> acknowledged(registry) > acknowledgedBefore);
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
