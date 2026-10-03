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

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * The quickstart with its consumers declared in {@code application.yaml} rather than built in
 * code.
 *
 * <ul>
 *   <li>{@link InvoiceService} saves an invoice and sends two messages in one transaction.
 *   <li>{@link InvoiceHandler} and {@link AuditBatchHandler} are ordinary beans implementing a
 *       handler interface.
 *   <li>{@code pgmq.consumers} names a queue and a handler bean for each, and tunes it. The starter
 *       builds, starts and instruments the containers - there is no {@code @Bean} for one.
 * </ul>
 *
 * <p>Run it with {@code ./gradlew :samples:sample-quickstart-declarative:run} against a Postgres
 * that has PGMQ installed:
 *
 * <pre>{@code
 * docker run -d --name pgmq -e POSTGRES_PASSWORD=postgres -p 5432:5432 \
 *     ghcr.io/pgmq/pg17-pgmq:v1.13.0
 * }</pre>
 */
@SpringBootApplication
public class DeclarativeQuickstartApplication {

    public static void main(String[] args) {
        SpringApplication.run(DeclarativeQuickstartApplication.class, args);
    }
}
