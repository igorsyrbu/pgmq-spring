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

package io.github.pgmqspring.samples.quickstart;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;

import io.github.pgmqspring.core.PgmqMessage;
import io.github.pgmqspring.core.client.PgmqOperations;

/**
 * The consuming side.
 *
 * <p>The handler is idempotent, which is not optional: PGMQ gives at-least-once delivery, so a
 * handler that finishes its work and then crashes before acknowledging will see the same message
 * again. Here {@code on conflict do nothing} makes a repeat delivery harmless.
 */
@Component
public class OrderHandler {

    private static final Log logger = LogFactory.getLog(OrderHandler.class);

    private final JdbcClient jdbc;

    private final PgmqOperations pgmq;

    public OrderHandler(JdbcClient jdbc, PgmqOperations pgmq) {
        this.jdbc = jdbc;
        this.pgmq = pgmq;
    }

    /** Handles one order event. Throwing signals failure and triggers the configured retry. */
    public void handle(PgmqMessage<OrderPlaced> message) {
        OrderPlaced event = message.payload();
        logger.info("Confirming order " + event.orderId() + " for " + event.customer()
                + " (delivery " + message.readCount() + ")");

        if (event.totalCents() < 0) {
            throw new IllegalArgumentException("order " + event.orderId() + " has a negative total");
        }

        int confirmed = this.jdbc.sql("insert into order_confirmations(order_id, confirmed_at) values (?, now()) "
                        + "on conflict (order_id) do nothing")
                .param(event.orderId())
                .update();

        // Runs in the container's transaction, so the event is published exactly when the
        // confirmation commits - and a repeat delivery, which confirms nothing, publishes nothing.
        if (confirmed == 1) {
            this.pgmq.send(NotificationHandler.QUEUE, new OrderConfirmed(event.orderId(), event.customer()));
        }
    }
}
