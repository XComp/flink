/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.util.concurrent;

import org.apache.flink.api.common.time.Deadline;

import java.time.Duration;

/** {@code DeadlineBasedRetryStrategy} implements {@link RetryStrategy} for a given deadline. */
public class DeadlineBasedRetryStrategy implements RetryStrategy {
    private final Deadline deadline;
    private final Duration retryDelay;

    public DeadlineBasedRetryStrategy(Deadline deadline, Duration retryDelay) {
        this.deadline = deadline;
        this.retryDelay = retryDelay;
    }

    @Override
    public int getNumRemainingRetries() {
        return deadline.hasTimeLeft() ? 1 : 0;
    }

    @Override
    public Duration getRetryDelay() {
        return retryDelay;
    }

    @Override
    public RetryStrategy getNextRetryStrategy() {
        return this;
    }
}
