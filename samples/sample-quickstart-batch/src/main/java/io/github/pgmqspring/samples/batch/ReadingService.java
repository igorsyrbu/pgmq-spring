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

import java.util.List;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import io.github.pgmqspring.core.client.PgmqOperations;

/**
 * The producing side: record an ingest request and enqueue all of its readings, atomically.
 *
 * <p>{@code sendBatch} writes every message in a single statement - one round trip however many
 * readings there are, up to {@code pgmq.producer.max-batch-size}, beyond which it uses one
 * statement per 500 - and, like {@code send}, joins the surrounding transaction. Either the
 * ingestion row and all of its messages commit, or none of them do.
 */
@Service
public class ReadingService {

    /** Queue the readings are published to. */
    public static final String QUEUE = "readings";

    private final JdbcClient jdbc;

    private final PgmqOperations pgmq;

    public ReadingService(JdbcClient jdbc, PgmqOperations pgmq) {
        this.jdbc = jdbc;
        this.pgmq = pgmq;
    }

    /**
     * Records an ingest request and publishes its readings.
     *
     * @return the ids PGMQ assigned, in the order of {@code readings}
     */
    @Transactional
    public List<Long> ingest(String ingestionId, List<Reading> readings) {
        this.jdbc.sql("insert into ingestions(id, reading_ct) values (?, ?)")
                .params(ingestionId, readings.size())
                .update();
        // One statement per 500 readings. If anything after this throws, nothing was sent.
        return this.pgmq.sendBatch(QUEUE, readings);
    }
}
