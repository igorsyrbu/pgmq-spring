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

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;

import io.github.pgmqspring.core.PgmqMessage;
import io.github.pgmqspring.core.consumer.PgmqBatchMessageHandler;

/**
 * Records the audit trail a batch at a time. Implementing {@link PgmqBatchMessageHandler} is all it
 * takes for the declared consumer to hand it whole batches.
 */
@Component
public class AuditBatchHandler implements PgmqBatchMessageHandler<AuditEntry> {

    private final JdbcClient jdbc;

    private final AtomicInteger largestBatch = new AtomicInteger();

    public AuditBatchHandler(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public void handle(List<PgmqMessage<AuditEntry>> batch) {
        this.largestBatch.accumulateAndGet(batch.size(), Math::max);
        for (PgmqMessage<AuditEntry> message : batch) {
            this.jdbc.sql("insert into audit_log(message_id, subject, action) values (?, ?, ?) "
                            + "on conflict (message_id) do nothing")
                    .params(message.id(), message.payload().subject(), message.payload().action())
                    .update();
        }
    }

    /** The most messages handed over in one call so far. */
    public int largestBatch() {
        return this.largestBatch.get();
    }
}
