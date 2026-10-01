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

package io.github.pgmqspring.core;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 * Tests for {@link QueueNames}, which mirrors the rules PGMQ enforces in
 * {@code validate_queue_name()} and {@code format_table_name()}.
 */
class QueueNamesTests {

    @ParameterizedTest
    @ValueSource(strings = {"orders", "order_events", "a", "Orders", "queue-with-dash", "q123"})
    void acceptsNamesPgmqAccepts(String name) {
        assertThat(QueueNames.validate(name)).isEqualTo(name);
    }

    @Test
    void acceptsExactlyMaxLength() {
        String name = "q".repeat(QueueNames.MAX_LENGTH);
        assertThat(QueueNames.validate(name)).hasSize(47);
    }

    @Test
    void rejectsNameOverMaxLength() {
        String name = "q".repeat(QueueNames.MAX_LENGTH + 1);
        assertThatExceptionOfType(InvalidQueueNameException.class)
                .isThrownBy(() -> QueueNames.validate(name))
                .withMessageContaining("maximum length is 47");
    }

    @ParameterizedTest
    @ValueSource(strings = {"has$dollar", "has;semicolon", "has--comment", "has'quote"})
    void rejectsCharactersPgmqForbids(String name) {
        assertThatExceptionOfType(InvalidQueueNameException.class)
                .isThrownBy(() -> QueueNames.validate(name))
                .satisfies((ex) -> assertThat(ex.getQueueName()).isEqualTo(name));
    }

    @Test
    void rejectsEmptyAndBlank() {
        assertThatExceptionOfType(InvalidQueueNameException.class).isThrownBy(() -> QueueNames.validate(""));
        assertThatExceptionOfType(InvalidQueueNameException.class).isThrownBy(() -> QueueNames.validate("   "));
    }

    @Test
    void normalizeFoldsCaseBecausePgmqLowercasesTableNames() {
        assertThat(QueueNames.normalize("Orders")).isEqualTo(QueueNames.normalize("orders"));
    }
}
