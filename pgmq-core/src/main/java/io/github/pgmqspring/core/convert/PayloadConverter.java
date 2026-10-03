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

package io.github.pgmqspring.core.convert;

/**
 * Converts application payloads to and from the JSON text PGMQ stores in its {@code jsonb} column.
 *
 * <p>PGMQ payloads must be valid JSON, so every payload passes through an implementation of this
 * interface. The library never hard-depends on a particular Jackson major version: the default
 * implementation is chosen at runtime, and applications can supply their own by defining a
 * {@code PayloadConverter} bean.
 *
 * <h2>The {@code String} contract</h2>
 *
 * <p>The two directions treat {@link String} differently, on purpose:
 *
 * <ul>
 *   <li>{@link #fromJson(String, Class)} with {@code String.class} (or {@code CharSequence.class})
 *       <strong>must</strong> return the stored JSON text verbatim, rather than requiring the
 *       payload to be a JSON string literal. That is what lets
 *       {@code read(queue, opts, String.class)} hand back the raw document for pass-through
 *       scenarios such as relaying or auditing.
 *   <li>{@link #toJson(Object)} serializes a {@code String} like any other value, as a JSON string
 *       literal. To store a document that is already JSON, callers use {@code sendRaw}, which
 *       bypasses the converter.
 * </ul>
 */
public interface PayloadConverter {

    /**
     * Serializes a payload to JSON text.
     *
     * @param payload the payload, never {@code null}
     * @return valid JSON text
     * @throws PayloadConversionException if serialization fails
     */
    String toJson(Object payload);

    /**
     * Deserializes JSON text into {@code type}.
     *
     * @param json the stored JSON document
     * @param type the target type; {@code String.class} and {@code CharSequence.class} mean "give
     *     me the raw document", while {@code Object.class} parses it into maps, lists and scalars
     * @param <T> the target type
     * @return the deserialized payload
     * @throws PayloadConversionException if deserialization fails
     */
    <T> T fromJson(String json, Class<T> type);
}
