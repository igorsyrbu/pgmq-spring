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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Ingests a few hundred readings on startup so that running the sample shows something.
 *
 * <p>Among them are one invalid reading, which is rejected without failing its batch, and one from
 * {@link ReadingBatchHandler#FLAKY_DEVICE}, whose whole batch fails once and is then redelivered.
 *
 * <p>Disabled with {@code sample.demo.enabled=false}, which is what the integration tests do.
 */
@Component
@ConditionalOnProperty(name = "sample.demo.enabled", havingValue = "true", matchIfMissing = true)
public class DemoRunner implements ApplicationRunner {

    private static final Log logger = LogFactory.getLog(DemoRunner.class);

    private final ReadingService readingService;

    public DemoRunner(ReadingService readingService) {
        this.readingService = readingService;
    }

    @Override
    public void run(ApplicationArguments args) {
        for (int request = 1; request <= 3; request++) {
            List<Reading> readings = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                readings.add(new Reading("demo-" + request + "-" + i, "sensor-" + (i % 5), 20 + i * 0.1));
            }
            if (request == 2) {
                readings.set(10, new Reading("demo-2-invalid", "sensor-0", 9999));
                readings.set(20, new Reading("demo-2-flaky", ReadingBatchHandler.FLAKY_DEVICE, 1.0));
            }
            List<Long> ids = this.readingService.ingest("demo-ingestion-" + request, readings);
            logger.info("Ingestion " + request + ": sent " + ids.size() + " readings in one statement");
        }
        logger.info("Inspect the results with:");
        logger.info("  select count(*) from readings;");
        logger.info("  select * from rejected_readings;");
    }
}
