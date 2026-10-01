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

import java.util.LinkedHashMap;
import java.util.Map;

import org.jspecify.annotations.Nullable;
import org.springframework.util.Assert;

/**
 * One message of a batch sent with {@link PgmqOperations#sendMessages(String, java.util.List)}:
 * a payload and the headers that belong to it alone.
 *
 * <p>This is what lets one batch carry messages for different FIFO groups, trace ids or tenants.
 * Instances are immutable; every mutator returns a copy.
 *
 * <pre>{@code
 * pgmq.sendMessages("user_events", events.stream()
 *         .map((event) -> OutboundMessage.of(event).group(event.userId()))
 *         .toList());
 * }</pre>
 *
 * @param payload the payload - an object for the configured converter to serialize, or, when
 *     {@code json} is {@code true}, an already-serialized JSON document stored verbatim
 * @param headers this message's headers; empty for none
 * @param json whether {@code payload} is a JSON document rather than an object to serialize
 */
public record OutboundMessage(Object payload, Map<String, Object> headers, boolean json) {

    public OutboundMessage {
        Assert.notNull(payload, "payload must not be null");
        Assert.notNull(headers, "headers must not be null");
        if (json) {
            Assert.isInstanceOf(String.class, payload, "a pre-serialized payload must be a String");
            Assert.hasText((String) payload, "a pre-serialized payload must not be empty");
        }
        headers = Map.copyOf(headers);
    }

    /** A message whose payload the configured converter serializes. */
    public static OutboundMessage of(Object payload) {
        return new OutboundMessage(payload, Map.of(), false);
    }

    /**
     * A message whose payload is already a JSON document, stored verbatim.
     *
     * <p>The batch counterpart of {@link PgmqOperations#sendRaw}: passing a JSON string to
     * {@link #of(Object)} would serialize it again, storing an escaped string literal.
     */
    public static OutboundMessage ofJson(String json) {
        return new OutboundMessage(json, Map.of(), true);
    }

    /** Returns a copy with the supplied headers, replacing any already set. */
    public OutboundMessage withHeaders(Map<String, Object> newHeaders) {
        return new OutboundMessage(this.payload, newHeaders, this.json);
    }

    /** Returns a copy with one additional header. Header values must be JSON-serializable. */
    public OutboundMessage withHeader(String name, Object value) {
        Assert.hasText(name, "header name must not be empty");
        Assert.notNull(value, "header value must not be null");
        Map<String, Object> merged = new LinkedHashMap<>(this.headers);
        merged.put(name, value);
        return new OutboundMessage(this.payload, merged, this.json);
    }

    /**
     * Returns a copy assigning this message to a FIFO group, by setting the
     * {@link FifoGroups#GROUP_HEADER} header.
     */
    public OutboundMessage group(String key) {
        Assert.hasText(key, "group key must not be empty");
        return withHeader(FifoGroups.GROUP_HEADER, key);
    }

    /** The header value for {@code name}, or {@code null}. */
    public @Nullable Object header(String name) {
        return this.headers.get(name);
    }
}
