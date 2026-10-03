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

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * The smallest application that does something useful with PGMQ.
 *
 * <p>Deliberately minimal, so it can be read in one sitting and copied as a starting point:
 *
 * <ul>
 *   <li>{@link OrderService} saves a row and sends a message in one transaction.
 *   <li>{@link OrderConsumerConfiguration} declares the listener containers: a transactional one
 *       with a transaction timeout, retries with backoff and dead-lettering, and one that
 *       acknowledges in batches and wakes on insert notifications.
 *   <li>A default header on every message sent, and the health indicator and metrics the starter
 *       contributes, come from configuration alone.
 *   <li>No HTTP layer and no chunking.
 * </ul>
 *
 * <p>For sending and consuming in batches see {@code sample-quickstart-batch}.
 *
 * <p>Run it with {@code ./gradlew :samples:sample-quickstart:run} against a Postgres that has
 * PGMQ installed:
 *
 * <pre>{@code
 * docker run -d --name pgmq -e POSTGRES_PASSWORD=postgres -p 5432:5432 \
 *     ghcr.io/pgmq/pg17-pgmq:v1.13.0
 * }</pre>
 */
@SpringBootApplication
public class QuickstartApplication {

    public static void main(String[] args) {
        SpringApplication.run(QuickstartApplication.class, args);
    }
}
