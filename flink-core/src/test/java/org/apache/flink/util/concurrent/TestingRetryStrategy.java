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

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/** {@code TestingRetryStrategy} is a testing implementation of {@link RetryStrategy}. */
public class TestingRetryStrategy implements RetryStrategy {
    private final Supplier<Integer> getNumRemainingRetriesSupplier;
    private final Supplier<Duration> getRetryDelaySupplier;

    private TestingRetryStrategy(
            Supplier<Integer> getNumRemainingRetriesSupplier,
            Supplier<Duration> getRetryDelaySupplier) {
        this.getNumRemainingRetriesSupplier = getNumRemainingRetriesSupplier;
        this.getRetryDelaySupplier = getRetryDelaySupplier;
    }

    @Override
    public int getNumRemainingRetries() {
        return getNumRemainingRetriesSupplier.get();
    }

    @Override
    public Duration getRetryDelay() {
        return getRetryDelaySupplier.get();
    }

    @Override
    public RetryStrategy getNextRetryStrategy() {
        return new TestingRetryStrategy(getNumRemainingRetriesSupplier, getRetryDelaySupplier);
    }

    public static Builder newBuilder(AtomicBoolean shouldRetry) {
        return new Builder().setGetNumRemainingRetriesSupplier(() -> shouldRetry.get() ? 1 : 0);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /** {@code Builder} for {@code TestingRetryStrategy} instances. */
    public static class Builder {
        private Supplier<Integer> getNumRemainingRetriesSupplier = () -> 0;
        private Supplier<Duration> getRetryDelaySupplier = () -> Duration.ZERO;

        private Builder() {
            // nothing to do
        }

        public Builder setGetNumRemainingRetriesSupplier(
                Supplier<Integer> getNumRemainingRetriesSupplier) {
            this.getNumRemainingRetriesSupplier = getNumRemainingRetriesSupplier;
            return this;
        }

        public Builder setGetRetryDelaySupplier(Supplier<Duration> getRetryDelaySupplier) {
            this.getRetryDelaySupplier = getRetryDelaySupplier;
            return this;
        }

        public TestingRetryStrategy build() {
            return new TestingRetryStrategy(getNumRemainingRetriesSupplier, getRetryDelaySupplier);
        }
    }
}
