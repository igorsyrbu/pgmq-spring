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

/**
 * How a grouped read distributes a batch across FIFO groups.
 *
 * <p>All three strategies enforce the same core guarantee - one in-flight message per group,
 * delivered in order - and differ only in how a single read's quantity budget is spread over the
 * groups that have work waiting. The descriptions below were confirmed by running each function
 * against PGMQ 1.13.0 with three messages in group {@code A} and two in group {@code B}, reading
 * with a quantity of ten.
 *
 * @see FifoGroups
 */
public enum GroupReadStrategy {

    /**
     * Fill the batch group by group, oldest waiting group first. The default.
     *
     * <p>Backed by {@code pgmq.read_grouped()}. In the reference scenario it returns
     * {@code A1, A2, A3, B1, B2} - it drains one group before moving to the next, which maximises
     * throughput per round trip but lets a busy group dominate a batch.
     */
    GREEDY,

    /**
     * Return at most one message per group per read.
     *
     * <p>Backed by {@code pgmq.read_grouped_head()}. In the reference scenario it returns only
     * {@code A1, B1}, however large the requested quantity. This is the easiest to reason about,
     * because a batch can never contain two messages that must be processed in order relative to
     * each other - so per-key ordering holds regardless of how the batch is then handled.
     */
    HEAD,

    /**
     * Spread the batch evenly across groups, one message per group per layer.
     *
     * <p>Backed by {@code pgmq.read_grouped_rr()}. In the reference scenario it returns
     * {@code A1, B1, A2, B2, A3} - every group gets a message before any group gets a second, so
     * one busy key cannot starve the others.
     */
    ROUND_ROBIN
}
