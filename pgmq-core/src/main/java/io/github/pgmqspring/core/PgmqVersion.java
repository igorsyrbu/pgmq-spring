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

import java.util.Comparator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jspecify.annotations.Nullable;

/**
 * A parsed PGMQ version, ordered by major/minor/patch.
 *
 * <p>PGMQ does not expose a {@code version()} SQL function, so the version is read from
 * {@code pg_extension.extversion}. When PGMQ was installed with its SQL-only (non-extension)
 * method there is no version marker at all; in that case {@link PgmqCapabilities} falls back to
 * probing the catalog for individual features rather than comparing version numbers.
 *
 * @param major the major version
 * @param minor the minor version
 * @param patch the patch version
 */
public record PgmqVersion(int major, int minor, int patch) implements Comparable<PgmqVersion> {

    private static final Pattern PATTERN = Pattern.compile("^(\\d+)\\.(\\d+)(?:\\.(\\d+))?");

    private static final Comparator<PgmqVersion> COMPARATOR =
            Comparator.comparingInt(PgmqVersion::major)
                    .thenComparingInt(PgmqVersion::minor)
                    .thenComparingInt(PgmqVersion::patch);

    /**
     * The oldest PGMQ release this library supports.
     *
     * <p>1.5.0 is the release that introduced message headers and the {@code headers} column on
     * {@code pgmq.message_record}; everything older is missing a feature the client exposes as
     * part of its core API.
     */
    public static final PgmqVersion MINIMUM_SUPPORTED = new PgmqVersion(1, 5, 0);

    /** Parses a version string such as {@code "1.13.0"}, or returns {@code null} if unparseable. */
    public static @Nullable PgmqVersion parse(@Nullable String value) {
        if (value == null) {
            return null;
        }
        Matcher matcher = PATTERN.matcher(value.trim());
        if (!matcher.find()) {
            return null;
        }
        String patch = matcher.group(3);
        try {
            return new PgmqVersion(
                    Integer.parseInt(matcher.group(1)),
                    Integer.parseInt(matcher.group(2)),
                    patch != null ? Integer.parseInt(patch) : 0);
        }
        catch (NumberFormatException ex) {
            return null;
        }
    }

    /** Returns {@code true} if this version is greater than or equal to {@code other}. */
    public boolean isAtLeast(PgmqVersion other) {
        return compareTo(other) >= 0;
    }

    @Override
    public int compareTo(PgmqVersion other) {
        return COMPARATOR.compare(this, other);
    }

    @Override
    public String toString() {
        return this.major + "." + this.minor + "." + this.patch;
    }
}
