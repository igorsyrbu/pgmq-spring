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
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Places a few orders on startup so that running the sample actually shows something.
 *
 * <p>Two of them succeed. The third has a negative total, which {@link OrderHandler} rejects, so
 * it is retried three times and then dead-lettered - the whole happy path and failure path in one
 * run.
 *
 * <p>Disabled with {@code sample.demo.enabled=false}, which is what the integration tests do so
 * they can control their own data.
 */
@Component
@ConditionalOnProperty(name = "sample.demo.enabled", havingValue = "true", matchIfMissing = true)
public class DemoRunner implements ApplicationRunner {

    private static final Log logger = LogFactory.getLog(DemoRunner.class);

    private final OrderService orderService;

    public DemoRunner(OrderService orderService) {
        this.orderService = orderService;
    }

    @Override
    public void run(ApplicationArguments args) {
        logger.info("Placing demo orders - watch for confirmations, then one dead-letter");
        this.orderService.placeOrder("demo-1", "alice", 2500);
        this.orderService.placeOrder("demo-2", "bob", 1800);
        this.orderService.placeOrder("demo-3-bad", "mallory", -1);
        logger.info("Demo orders placed. Inspect them with:");
        logger.info("  select * from order_confirmations;");
        logger.info("  select * from pgmq.q_orders_dlq;");
    }
}
