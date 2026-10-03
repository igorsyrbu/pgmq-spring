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

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import io.github.pgmqspring.core.client.PgmqOperations;

/**
 * The producing side: save an invoice and publish it, atomically - to the queue that emails it and
 * to the audit trail.
 */
@Service
public class InvoiceService {

    /** Queue that issued invoices are published to. */
    public static final String INVOICES = "invoices";

    /** Queue the audit trail is published to. */
    public static final String AUDIT = "audit";

    private final JdbcClient jdbc;

    private final PgmqOperations pgmq;

    public InvoiceService(JdbcClient jdbc, PgmqOperations pgmq) {
        this.jdbc = jdbc;
        this.pgmq = pgmq;
    }

    /**
     * Issues an invoice: the row and both messages commit together, or none of them do.
     *
     * @return the id PGMQ assigned to the invoice message
     */
    @Transactional
    public long issue(String invoiceId, String customer, long totalCents) {
        this.jdbc.sql("insert into invoices(id, customer, total_cents) values (?, ?, ?)")
                .params(invoiceId, customer, totalCents)
                .update();
        this.pgmq.send(AUDIT, new AuditEntry(invoiceId, "issued"));
        return this.pgmq.send(INVOICES, new InvoiceIssued(invoiceId, customer, totalCents));
    }
}
