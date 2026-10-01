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

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * The quickstart, in batches.
 *
 * <ul>
 *   <li>{@link ReadingService} records an ingest request and sends all of its readings in <em>one</em>
 *       statement, in one transaction.
 *   <li>{@link ReadingConsumerConfiguration} consumes them up to 50 at a time, and
 *       {@link ReadingBatchHandler} writes each batch with one multi-row insert.
 * </ul>
 *
 * <p>For one message at a time see {@code sample-quickstart}. Run with
 * {@code ./gradlew :samples:sample-quickstart-batch:run} against a Postgres with PGMQ installed:
 *
 * <pre>{@code
 * docker run -d --name pgmq -e POSTGRES_PASSWORD=postgres -p 5432:5432 \
 *     ghcr.io/pgmq/pg17-pgmq:v1.13.0
 * }</pre>
 */
@SpringBootApplication
public class BatchQuickstartApplication {

    public static void main(String[] args) {
        SpringApplication.run(BatchQuickstartApplication.class, args);
    }
}
