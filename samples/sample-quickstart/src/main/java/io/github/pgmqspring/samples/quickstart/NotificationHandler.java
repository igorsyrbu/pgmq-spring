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

/**
 * Notifies customers of confirmed orders.
 *
 * <p>Its only write is one idempotent insert, so unlike {@link OrderHandler} it needs no
 * transaction - and that is what lets its container batch the acknowledgements: the messages of a
 * polled batch are deleted with one statement instead of one each.
 */
@Component
public class NotificationHandler {

    /** Queue that confirmed orders are published to. */
    public static final String QUEUE = "order_notifications";

    private static final Log logger = LogFactory.getLog(NotificationHandler.class);

    private final JdbcClient jdbc;

    public NotificationHandler(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public void handle(PgmqMessage<OrderConfirmed> message) {
        OrderConfirmed event = message.payload();
        logger.info("Notifying " + event.customer() + " that order " + event.orderId() + " is confirmed");
        this.jdbc.sql("insert into order_notifications(order_id, notified_at) values (?, now()) "
                        + "on conflict (order_id) do nothing")
                .param(event.orderId())
                .update();
    }
}
