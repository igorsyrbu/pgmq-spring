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

package io.github.pgmqspring.autoconfigure;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.support.StaticListableBeanFactory;
import org.springframework.boot.health.contributor.Status;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DelegatingDataSource;

import io.github.pgmqspring.core.PgmqContainerSupport;
import io.github.pgmqspring.core.client.PgmqOperations;
import io.github.pgmqspring.core.client.PgmqTemplate;
import io.github.pgmqspring.core.convert.JacksonPayloadConverter;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Startup and health behaviour under real-world conditions: an outage after startup, and several
 * instances starting at the same moment.
 */
class StartupAndHealthHardeningTests {

    /** A data source whose connections can be made to fail, simulating a database outage. */
    static final class SwitchableDataSource extends DelegatingDataSource {

        final AtomicBoolean down = new AtomicBoolean();

        SwitchableDataSource() {
            super(PgmqContainerSupport.dataSource());
        }

        @Override
        public Connection getConnection() throws SQLException {
            if (this.down.get()) {
                throw new SQLException("simulated outage", "08001");
            }
            return super.getConnection();
        }
    }

    @Test
    void healthReportsDownDuringAnOutageEvenAfterCapabilitiesWereCached() {
        SwitchableDataSource dataSource = new SwitchableDataSource();
        PgmqTemplate pgmq = new PgmqTemplate(new JdbcTemplate(dataSource), new JacksonPayloadConverter());
        PgmqHealthIndicator indicator = new PgmqHealthIndicator(pgmq, new PgmqProperties());

        assertThat(indicator.health().getStatus()).isEqualTo(Status.UP);
        dataSource.down.set(true);
        assertThat(indicator.health().getStatus()).isEqualTo(Status.DOWN);
        dataSource.down.set(false);
        assertThat(indicator.health().getStatus()).isEqualTo(Status.UP);
    }

    @Test
    // Postgres does not guarantee concurrent CREATE ... IF NOT EXISTS to be safe, so the
    // initializer re-checks after a failed creation; this proves several creators still succeed.
    void severalInstancesCanCreateTheSameQueuesAtTheSameTime() throws Exception {
        String queue = PgmqContainerSupport.uniqueQueueName("concurrent_create");
        PgmqProperties properties = new PgmqProperties();
        PgmqProperties.Queue entry = new PgmqProperties.Queue();
        entry.setName(queue);
        // The grouped-read index exists from PGMQ 1.10.0; the suite also runs on the 1.5.1 floor.
        boolean fifo = new PgmqTemplate(PgmqContainerSupport.dataSource(), new JacksonPayloadConverter())
                .capabilities().groupedReads();
        entry.setFifoIndex(fifo);
        properties.setQueues(List.of(entry));

        int instances = 8;
        CountDownLatch go = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(instances);
        try {
            List<Future<?>> results = new ArrayList<>();
            for (int i = 0; i < instances; i++) {
                // Each "instance" has its own client, as separate JVMs would.
                PgmqTemplate pgmq = new PgmqTemplate(PgmqContainerSupport.dataSource(), new JacksonPayloadConverter());
                PgmqInitializer initializer =
                        new PgmqInitializer(properties, provider(pgmq), PgmqContainerSupport.dataSource());
                results.add(pool.submit(() -> {
                    go.await();
                    initializer.afterPropertiesSet();
                    return null;
                }));
            }
            go.countDown();
            for (Future<?> result : results) {
                result.get(60, TimeUnit.SECONDS);
            }
        }
        finally {
            pool.shutdownNow();
        }
        JdbcTemplate jdbc = new JdbcTemplate(PgmqContainerSupport.dataSource());
        assertThat(jdbc.queryForObject("select count(*) from pgmq.list_queues() where queue_name = ?",
                Integer.class, queue)).isEqualTo(1);
        assertThat(jdbc.queryForObject("select count(*) from pg_indexes where schemaname = 'pgmq' "
                + "and tablename = ? and indexname = ?", Integer.class, "q_" + queue, "q_" + queue + "_fifo_idx"))
                .isEqualTo(fifo ? 1 : 0);
    }

    private static ObjectProvider<PgmqOperations> provider(PgmqOperations pgmq) {
        StaticListableBeanFactory beanFactory = new StaticListableBeanFactory();
        beanFactory.addBean("pgmqTemplate", pgmq);
        return beanFactory.getBeanProvider(PgmqOperations.class);
    }
}
