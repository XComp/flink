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

package org.apache.flink.runtime.dispatcher.cleanup;

import org.apache.flink.api.common.JobID;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.Executors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;

import static org.assertj.core.api.Assertions.assertThat;

/** {@code DefaultResourceCleanerTest} tests {@link DefaultResourceCleaner}. */
public class DefaultResourceCleanerTest {

    private static final Executor EXECUTOR = Executors.directExecutor();
    private static final JobID JOB_ID = new JobID();

    private DefaultResourceCleaner testInstance;
    private CleanupCallbackWithTrigger cleanup0;
    private CleanupCallbackWithTrigger cleanup1;

    @BeforeEach
    public void setup() {
        cleanup0 = new CleanupCallbackWithTrigger();
        cleanup1 = new CleanupCallbackWithTrigger();

        testInstance = new DefaultResourceCleaner(EXECUTOR);
        testInstance.withCleanupOf(cleanup0).withCleanupOf(cleanup1);
    }

    @Test
    public void testSuccessfulConcurrentCleanup() {
        CompletableFuture<Void> cleanupResult = testInstance.cleanupAsync(JOB_ID);

        assertThat(cleanupResult).isNotCompleted();
        assertThat(cleanup0)
                .extracting(CleanupCallbackWithTrigger::getProcessedJobId)
                .isEqualTo(JOB_ID);
        assertThat(cleanup1)
                .extracting(CleanupCallbackWithTrigger::getProcessedJobId)
                .isEqualTo(JOB_ID);

        cleanup0.completeCleanup();
        assertThat(cleanupResult).isNotCompleted();

        cleanup1.completeCleanup();
        assertThat(cleanupResult).isCompleted();
    }

    @Test
    public void testConcurrentCleanupWithExceptionFirst() {
        CompletableFuture<Void> cleanupResult = testInstance.cleanupAsync(JOB_ID);

        assertThat(cleanupResult).isNotCompleted();
        assertThat(cleanup0)
                .extracting(CleanupCallbackWithTrigger::getProcessedJobId)
                .isEqualTo(JOB_ID);
        assertThat(cleanup1)
                .extracting(CleanupCallbackWithTrigger::getProcessedJobId)
                .isEqualTo(JOB_ID);

        final RuntimeException expectedException = new RuntimeException("Expected exception");
        cleanup0.completeCleanupExceptionally(expectedException);
        assertThat(cleanupResult).isNotCompleted();

        cleanup1.completeCleanup();
        assertThat(cleanupResult)
                .failsWithin(Duration.ZERO)
                .withThrowableOfType(ExecutionException.class)
                .withCause(expectedException);
    }

    @Test
    public void testConcurrentCleanupWithExceptionSecond() {
        CompletableFuture<Void> cleanupResult = testInstance.cleanupAsync(JOB_ID);

        assertThat(cleanupResult).isNotCompleted();
        assertThat(cleanup0)
                .extracting(CleanupCallbackWithTrigger::getProcessedJobId)
                .isEqualTo(JOB_ID);
        assertThat(cleanup1)
                .extracting(CleanupCallbackWithTrigger::getProcessedJobId)
                .isEqualTo(JOB_ID);

        cleanup0.completeCleanup();
        assertThat(cleanupResult).isNotCompleted();

        final RuntimeException expectedException = new RuntimeException("Expected exception");
        cleanup1.completeCleanupExceptionally(expectedException);
        assertThat(cleanupResult)
                .failsWithin(Duration.ZERO)
                .withThrowableOfType(ExecutionException.class)
                .withCause(expectedException);
    }

    private static class CleanupCallbackWithTrigger
            implements BiFunction<JobID, Executor, CompletableFuture<Void>> {

        private final CompletableFuture<Void> resultFuture = new CompletableFuture<>();
        private JobID jobId;

        @Override
        public CompletableFuture<Void> apply(JobID jobId, Executor executor) {
            Preconditions.checkState(this.jobId == null);
            this.jobId = jobId;

            return resultFuture;
        }

        public JobID getProcessedJobId() {
            return jobId;
        }

        public void completeCleanup() {
            this.resultFuture.complete(null);
        }

        public void completeCleanupExceptionally(Throwable expectedException) {
            this.resultFuture.completeExceptionally(expectedException);
        }
    }
}
