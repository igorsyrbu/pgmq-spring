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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;

/**
 * {@link PayloadConverter} backed by Jackson 2 ({@code com.fasterxml.jackson}).
 *
 * <p>Provided for applications that still run on a Jackson 2 {@code ObjectMapper}. Spring Boot 4
 * defaults to Jackson 3 and its Jackson 2 auto-configuration is deprecated for removal in 4.3, so
 * new applications should prefer {@link JacksonPayloadConverter}.
 */
public class Jackson2PayloadConverter implements PayloadConverter {

    private final ObjectMapper objectMapper;

    /**
     * Creates a converter with a default mapper that registers every Jackson module on the
     * classpath - notably {@code jackson-datatype-jsr310}, without which Jackson 2 cannot serialize
     * {@code java.time} types at all - and writes dates as ISO-8601 text, matching what Jackson 3
     * does by default.
     */
    public Jackson2PayloadConverter() {
        this(JsonMapper.builder()
                .findAndAddModules()
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .build());
    }

    public Jackson2PayloadConverter(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public String toJson(Object payload) {
        try {
            return this.objectMapper.writeValueAsString(payload);
        }
        catch (JsonProcessingException ex) {
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
        catch (JsonProcessingException ex) {
            throw new PayloadConversionException("Failed to deserialize JSON payload to " + type.getName(), ex);
        }
    }
}
