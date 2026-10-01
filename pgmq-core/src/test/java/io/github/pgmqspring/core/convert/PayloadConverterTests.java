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

import java.time.Instant;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 * Both Jackson generations behave the same way through {@link PayloadConverter}.
 */
class PayloadConverterTests {

    record Order(String id, int quantity, List<String> tags) {
    }

    static List<PayloadConverter> converters() {
        return List.of(new JacksonPayloadConverter(), new Jackson2PayloadConverter());
    }

    @ParameterizedTest
    @MethodSource("converters")
    void roundTripsARecord(PayloadConverter converter) {
        Order order = new Order("A-1", 3, List.of("x", "y"));
        String json = converter.toJson(order);
        assertThat(converter.fromJson(json, Order.class)).isEqualTo(order);
        assertThat(converter.fromJson(json, Map.class)).containsEntry("id", "A-1").containsEntry("quantity", 3);
    }

    @ParameterizedTest
    @MethodSource("converters")
    void hasTheRawDocumentForStringTargets(PayloadConverter converter) {
        assertThat(converter.fromJson("{\"a\":1}", String.class)).isEqualTo("{\"a\":1}");
    }

    @ParameterizedTest
    @MethodSource("converters")
    void reportsFailuresAsPayloadConversionException(PayloadConverter converter) {
        assertThatExceptionOfType(PayloadConversionException.class)
                .isThrownBy(() -> converter.fromJson("\"not an order\"", Order.class));
        assertThatExceptionOfType(PayloadConversionException.class)
                .isThrownBy(() -> converter.toJson(new Object() {
                    public Object getSelf() {
                        return this;
                    }
                }));
    }

    @ParameterizedTest
    @MethodSource("converters")
    void serializesJavaTimeWithoutExtraModules(PayloadConverter converter) {
        record Stamped(Instant at) {
        }
        String json = converter.toJson(new Stamped(Instant.parse("2026-01-02T03:04:05Z")));
        assertThat(json).contains("2026-01-02T03:04:05Z");
    }

    @ParameterizedTest
    @MethodSource("converters")
    void parsesObjectTargetsIntoMapsAndLists(PayloadConverter converter) {
        assertThat(converter.fromJson("{\"a\":1}", Object.class)).isInstanceOf(Map.class);
        assertThat(converter.fromJson("[1,2]", Object.class)).isInstanceOf(List.class);
        assertThat(converter.fromJson("{\"a\":1}", CharSequence.class)).isEqualTo("{\"a\":1}");
    }
}
