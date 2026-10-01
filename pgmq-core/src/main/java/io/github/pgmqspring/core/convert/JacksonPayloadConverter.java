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

import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/**
 * {@link PayloadConverter} backed by Jackson 3 ({@code tools.jackson}), the Jackson generation
 * Spring Boot 4 auto-configures by default.
 *
 * <p>Prefer constructing this with the application's own Spring-managed {@link ObjectMapper} so
 * that payloads honour the application's serialization settings (naming strategy, date format,
 * registered modules).
 */
public class JacksonPayloadConverter implements PayloadConverter {

    private final ObjectMapper objectMapper;

    /** Creates a converter using a default {@link JsonMapper}. */
    public JacksonPayloadConverter() {
        this(JsonMapper.builder().build());
    }

    /**
     * Creates a converter using the supplied mapper.
     *
     * @param objectMapper the mapper, typically the application's Spring-managed one
     */
    public JacksonPayloadConverter(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public String toJson(Object payload) {
        try {
            return this.objectMapper.writeValueAsString(payload);
        }
        catch (JacksonException ex) {
            throw new PayloadConversionException(
                    "Failed to serialize payload of type " + payload.getClass().getName() + " to JSON", ex);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T fromJson(String json, Class<T> type) {
        // Text targets get the stored document as-is; Object.class is parsed like any other type,
        // into maps, lists and scalars, as Jackson does everywhere else.
        if (type == String.class || type == CharSequence.class) {
            return (T) json;
        }
        try {
            return this.objectMapper.readValue(json, type);
        }
        catch (JacksonException ex) {
            throw new PayloadConversionException("Failed to deserialize JSON payload to " + type.getName(), ex);
        }
    }
}
