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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;

import io.github.pgmqspring.core.PgmqMessage;
import io.github.pgmqspring.core.consumer.PgmqMessageHandler;

/**
 * Emails issued invoices. An ordinary bean: {@code pgmq.consumers.invoices.handler} names it, and
 * its type argument tells the starter what to convert the payload to.
 *
 * <p>Idempotent, as at-least-once delivery requires: a repeat delivery inserts nothing.
 */
@Component
public class InvoiceHandler implements PgmqMessageHandler<InvoiceIssued> {

    private static final Log logger = LogFactory.getLog(InvoiceHandler.class);

    private final JdbcClient jdbc;

    public InvoiceHandler(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public void handle(PgmqMessage<InvoiceIssued> message) {
        InvoiceIssued invoice = message.payload();
        if (invoice.totalCents() <= 0) {
            throw new InvalidInvoiceException("invoice " + invoice.invoiceId() + " has nothing to pay");
        }
        logger.info("Emailing invoice " + invoice.invoiceId() + " to " + invoice.customer());
        this.jdbc.sql("insert into invoice_emails(invoice_id, sent_at) values (?, now()) "
                        + "on conflict (invoice_id) do nothing")
                .param(invoice.invoiceId())
                .update();
    }
}
