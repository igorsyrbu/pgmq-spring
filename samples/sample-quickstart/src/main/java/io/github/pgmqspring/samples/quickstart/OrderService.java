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

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import io.github.pgmqspring.core.client.PgmqOperations;

/**
 * The producing side: save a row and send a message, atomically.
 *
 * <p>This is the whole reason to use PGMQ rather than a separate broker. The insert and the send
 * are the same Postgres transaction, so there is no window in which one exists without the other,
 * and no outbox table or change-data-capture pipeline is needed to arrange that.
 */
@Service
public class OrderService {

    /** Queue that order events are published to. */
    public static final String QUEUE = "orders";

    private final JdbcClient jdbc;

    private final PgmqOperations pgmq;

    public OrderService(JdbcClient jdbc, PgmqOperations pgmq) {
        this.jdbc = jdbc;
        this.pgmq = pgmq;
    }

    /**
     * Places an order and publishes an event for it.
     *
     * @return the id PGMQ assigned to the message
     */
    @Transactional
    public long placeOrder(String orderId, String customer, long totalCents) {
        this.jdbc.sql("insert into orders(id, customer, total_cents) values (?, ?, ?)")
                .params(orderId, customer, totalCents)
                .update();

        // If anything after this line throws, the row and the message both disappear.
        return this.pgmq.send(QUEUE, new OrderPlaced(orderId, customer, totalCents));
    }
}
