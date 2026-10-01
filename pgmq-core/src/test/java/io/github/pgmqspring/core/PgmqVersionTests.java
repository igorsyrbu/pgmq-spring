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

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PgmqVersion} parsing and ordering. */
class PgmqVersionTests {

    @Test
    void parsesFullVersion() {
        assertThat(PgmqVersion.parse("1.13.0")).isEqualTo(new PgmqVersion(1, 13, 0));
    }

    @Test
    void parsesVersionWithoutPatch() {
        assertThat(PgmqVersion.parse("1.5")).isEqualTo(new PgmqVersion(1, 5, 0));
    }

    @Test
    void returnsNullForUnparseableInput() {
        assertThat(PgmqVersion.parse(null)).isNull();
        assertThat(PgmqVersion.parse("not-a-version")).isNull();
    }

    @Test
    void ordersNumericallyNotLexicographically() {
        // The bug this guards: "1.9.0" sorts after "1.13.0" as a string.
        assertThat(PgmqVersion.parse("1.13.0")).isGreaterThan(PgmqVersion.parse("1.9.0"));
    }

    @Test
    void isAtLeastIsInclusive() {
        assertThat(new PgmqVersion(1, 5, 0).isAtLeast(PgmqVersion.MINIMUM_SUPPORTED)).isTrue();
        assertThat(new PgmqVersion(1, 4, 5).isAtLeast(PgmqVersion.MINIMUM_SUPPORTED)).isFalse();
    }
}
