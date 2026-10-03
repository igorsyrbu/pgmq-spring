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

package io.github.pgmqspring.core.client;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.github.pgmqspring.core.PgmqContainerSupport;
import io.github.pgmqspring.core.PgmqMessage;
import io.github.pgmqspring.core.convert.JacksonPayloadConverter;

import static io.github.pgmqspring.core.PgmqContainerSupport.newQueue;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

/** {@link PgmqTemplate#setDefaultHeaders(Map)}: headers every send carries unless overridden. */
class DefaultHeadersIntegrationTests {

    private PgmqTemplate pgmq;

    @BeforeEach
    void setUp() {
        this.pgmq = new PgmqTemplate(PgmqContainerSupport.dataSource(), new JacksonPayloadConverter());
        this.pgmq.setDefaultHeaders(Map.of("source", "order-service", "schema", "v2"));
    }

    private List<Map<String, Object>> headersOf(String queue) {
        return this.pgmq.read(queue, ReadOptions.batch(10)).stream().map(PgmqMessage::headers).toList();
    }

    @Test
    void singleSendsCarryTheDefaults() {
        String queue = newQueue("default_headers_single");

        this.pgmq.send(queue, Map.of("n", 1));
        this.pgmq.sendRaw(queue, "{\"n\":2}", SendOptions.none());

        assertThat(headersOf(queue)).containsOnly(Map.of("source", "order-service", "schema", "v2"));
    }

    @Test
    void explicitHeadersWinOverTheDefaults() {
        String queue = newQueue("default_headers_override");

        this.pgmq.send(queue, Map.of("n", 1), SendOptions.headers(Map.of("schema", "v3", "trace", "t-1")));

        assertThat(headersOf(queue)).containsExactly(Map.of("source", "order-service", "schema", "v3", "trace", "t-1"));
    }

    @Test
    void batchesCarryTheDefaults() {
        String queue = newQueue("default_headers_batch");

        this.pgmq.sendBatch(queue, List.of(Map.of("n", 1), Map.of("n", 2)));
        this.pgmq.sendBatch(queue, List.of(Map.of("n", 3)), SendOptions.headers(Map.of("schema", "v3")));

        assertThat(headersOf(queue)).containsExactly(
                Map.of("source", "order-service", "schema", "v2"),
                Map.of("source", "order-service", "schema", "v2"),
                Map.of("source", "order-service", "schema", "v3"));
    }

    @Test
    void messagesWinOverSendOptionsWhichWinOverTheDefaults() {
        String queue = newQueue("default_headers_messages");

        this.pgmq.sendMessages(queue, List.of(
                OutboundMessage.of(Map.of("n", 1)),
                OutboundMessage.of(Map.of("n", 2)).withHeader("schema", "v4"),
                OutboundMessage.of(Map.of("n", 3)).group("g-1")),
                SendOptions.headers(Map.of("schema", "v3")));

        assertThat(headersOf(queue)).containsExactly(
                Map.of("source", "order-service", "schema", "v3"),
                Map.of("source", "order-service", "schema", "v4"),
                Map.of("source", "order-service", "schema", "v3", FifoGroups.GROUP_HEADER, "g-1"));
    }

    @Test
    void withoutDefaultsAMessageWithoutHeadersStoresNull() {
        String queue = newQueue("default_headers_none");
        this.pgmq.setDefaultHeaders(Map.of());

        this.pgmq.send(queue, Map.of("n", 1));
        this.pgmq.sendBatch(queue, List.of(Map.of("n", 2)));

        assertThat(headersOf(queue)).containsExactly(null, null);
    }

    @Test
    void rejectsNullValuesAndCopiesTheMap() {
        Map<String, Object> withNull = new HashMap<>();
        withNull.put("source", null);
        assertThatNullPointerException().isThrownBy(() -> this.pgmq.setDefaultHeaders(withNull));

        Map<String, Object> mutable = new HashMap<>(Map.of("source", "a"));
        this.pgmq.setDefaultHeaders(mutable);
        mutable.put("source", "b");
        assertThat(this.pgmq.getDefaultHeaders()).isEqualTo(Map.of("source", "a"));
    }
}
