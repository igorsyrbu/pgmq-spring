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
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Issues a few invoices on startup so that running the sample shows something: two are emailed,
 * and one, for nothing, is dead-lettered on its first failure. Disabled with
 * {@code sample.demo.enabled=false}, as the integration tests do.
 */
@Component
@ConditionalOnProperty(name = "sample.demo.enabled", havingValue = "true", matchIfMissing = true)
public class DemoRunner implements ApplicationRunner {

    private static final Log logger = LogFactory.getLog(DemoRunner.class);

    private final InvoiceService invoiceService;

    public DemoRunner(InvoiceService invoiceService) {
        this.invoiceService = invoiceService;
    }

    @Override
    public void run(ApplicationArguments args) {
        logger.info("Issuing demo invoices - watch for two emails, then one dead-letter");
        this.invoiceService.issue("inv-1", "alice", 2500);
        this.invoiceService.issue("inv-2", "bob", 1800);
        this.invoiceService.issue("inv-3-empty", "mallory", 0);
        logger.info("Demo invoices issued. Inspect them with:");
        logger.info("  select * from invoice_emails;");
        logger.info("  select * from audit_log;");
        logger.info("  select * from pgmq.q_invoices_dlq;");
    }
}
