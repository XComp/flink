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

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobmaster.JobManagerRunner;
import org.apache.flink.runtime.jobmaster.TestingJobManagerRunner;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.concurrent.Executors;
import org.apache.flink.util.function.BiFunctionWithException;

import org.assertj.core.api.ThrowingConsumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code JobManagerRunnerRegistryTest} tests the functionality of {@link JobManagerRunnerRegistry}.
 */
public class JobManagerRunnerRegistryTest {

    private JobManagerRunnerRegistry testInstance;

    @BeforeEach
    public void setup() {
        testInstance = new JobManagerRunnerRegistry(4);
    }

    @Test
    public void testIsRegistered() {
        final JobID jobId = new JobID();
        testInstance.register(TestingJobManagerRunner.newBuilder().setJobId(jobId).build());
        assertThat(testInstance.isRegistered(jobId)).isTrue();
    }

    @Test
    public void testIsNotRegistered() {
        assertThat(testInstance.isRegistered(new JobID())).isFalse();
    }

    @Test
    public void testRegister() {
        final JobID jobId = new JobID();
        testInstance.register(TestingJobManagerRunner.newBuilder().setJobId(jobId).build());
        assertThat(testInstance.isRegistered(jobId)).isTrue();
    }

    @Test
    public void testRegisteringTwiceCausesFailure() {
        final JobID jobId = new JobID();
        testInstance.register(TestingJobManagerRunner.newBuilder().setJobId(jobId).build());
        assertThat(testInstance.isRegistered(jobId)).isTrue();

        assertThatThrownBy(
                        () ->
                                testInstance.register(
                                        TestingJobManagerRunner.newBuilder()
                                                .setJobId(jobId)
                                                .build()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testGet() {
        final JobID jobId = new JobID();
        final JobManagerRunner jobManagerRunner =
                TestingJobManagerRunner.newBuilder().setJobId(jobId).build();
        testInstance.register(jobManagerRunner);

        assertThat(testInstance.get(jobId)).isEqualTo(jobManagerRunner);
    }

    @Test
    public void testGetOnNonExistingJobManagerRunner() {
        assertThatThrownBy(() -> testInstance.get(new JobID()))
                .isInstanceOf(NoSuchElementException.class);
    }

    @Test
    public void size() {
        assertThat(testInstance.size()).isEqualTo(0);
        testInstance.register(TestingJobManagerRunner.newBuilder().build());
        assertThat(testInstance.size()).isEqualTo(1);
        testInstance.register(TestingJobManagerRunner.newBuilder().build());
        assertThat(testInstance.size()).isEqualTo(2);
    }

    @Test
    public void testGetRunningJobIds() {
        assertThat(testInstance.getRunningJobIds()).isEmpty();

        final JobID jobId0 = new JobID();
        final JobID jobId1 = new JobID();
        testInstance.register(TestingJobManagerRunner.newBuilder().setJobId(jobId0).build());
        testInstance.register(TestingJobManagerRunner.newBuilder().setJobId(jobId1).build());

        assertThat(testInstance.getRunningJobIds()).containsExactlyInAnyOrder(jobId0, jobId1);
    }

    @Test
    public void testGetJobManagerRunners() {
        assertThat(testInstance.getJobManagerRunners()).isEmpty();

        final JobManagerRunner jobManagerRunner0 = TestingJobManagerRunner.newBuilder().build();
        final JobManagerRunner jobManagerRunner1 = TestingJobManagerRunner.newBuilder().build();
        testInstance.register(jobManagerRunner0);
        testInstance.register(jobManagerRunner1);

        assertThat(testInstance.getJobManagerRunners())
                .containsExactlyInAnyOrder(jobManagerRunner0, jobManagerRunner1);
    }

    @Test
    public void testSuccessfulGlobalCleanup() throws Throwable {
        testSuccessfulSynchronousCleanup(testInstance::globalCleanup);
    }

    @Test
    public void testSuccessfulLocalCleanup() throws Throwable {
        testSuccessfulSynchronousCleanup(testInstance::localCleanup);
    }

    private void testSuccessfulSynchronousCleanup(ThrowingConsumer<JobID> callback)
            throws Throwable {
        final TestingJobManagerRunner jobManagerRunner = registerTestingJobManagerRunner();

        callback.acceptThrows(jobManagerRunner.getJobID());
        assertThat(testInstance.isRegistered(jobManagerRunner.getJobID())).isFalse();
        assertThat(jobManagerRunner.getTerminationFuture()).isCompleted();
    }

    @Test
    public void testFailingGlobalCleanup() {
        testFailingSynchronousCleanup(testInstance::globalCleanup);
    }

    @Test
    public void testFailingLocalCleanup() {
        testFailingSynchronousCleanup(testInstance::localCleanup);
    }

    private void testFailingSynchronousCleanup(ThrowingConsumer<JobID> callback) {
        final TestingJobManagerRunner jobManagerRunner = registerTestingJobManagerRunner();

        assertThat(testInstance.isRegistered(jobManagerRunner.getJobID())).isTrue();
        assertThat(jobManagerRunner.getTerminationFuture()).isNotDone();

        final RuntimeException expectedException = new RuntimeException("Expected exception");
        jobManagerRunner.completeTerminationFutureExceptionally(expectedException);

        assertThatThrownBy(() -> callback.acceptThrows(jobManagerRunner.getJobID()))
                .isInstanceOf(FlinkException.class)
                .hasCause(expectedException);
        assertThat(testInstance.isRegistered(jobManagerRunner.getJobID())).isFalse();
    }

    @Test
    public void testSuccessfulGlobalCleanupAsync() throws Exception {
        testSuccessfulCleanupAsync(testInstance::globalCleanupAsync);
    }

    @Test
    public void testSuccessfulLocalCleanupAsync() throws Exception {
        testSuccessfulCleanupAsync(testInstance::localCleanupAsync);
    }

    private void testSuccessfulCleanupAsync(
            BiFunctionWithException<JobID, Executor, CompletableFuture<Void>, Exception> callback)
            throws Exception {
        final TestingJobManagerRunner jobManagerRunner = registerTestingJobManagerRunner();

        final CompletableFuture<Void> cleanupResult =
                callback.apply(jobManagerRunner.getJobID(), Executors.directExecutor());
        assertThat(testInstance.isRegistered(jobManagerRunner.getJobID())).isFalse();
        assertThat(cleanupResult).isCompleted();
    }

    @Test
    public void testFailingGlobalCleanupAsync() throws Exception {
        testFailingCleanupAsync(testInstance::globalCleanupAsync);
    }

    @Test
    public void testFailingLocalCleanupAsync() throws Exception {
        testFailingCleanupAsync(testInstance::localCleanupAsync);
    }

    private void testFailingCleanupAsync(
            BiFunctionWithException<JobID, Executor, CompletableFuture<Void>, Exception> callback)
            throws Exception {
        final TestingJobManagerRunner jobManagerRunner = registerTestingJobManagerRunner();

        assertThat(testInstance.isRegistered(jobManagerRunner.getJobID())).isTrue();
        assertThat(jobManagerRunner.getTerminationFuture()).isNotDone();

        final RuntimeException expectedException = new RuntimeException("Expected exception");
        jobManagerRunner.completeTerminationFutureExceptionally(expectedException);

        final CompletableFuture<Void> cleanupResult =
                callback.apply(jobManagerRunner.getJobID(), Executors.directExecutor());
        assertThat(testInstance.isRegistered(jobManagerRunner.getJobID())).isFalse();
        assertThat(cleanupResult)
                .isCompletedExceptionally()
                .failsWithin(Duration.ZERO)
                .withThrowableOfType(ExecutionException.class)
                .withCause(expectedException);
    }

    private TestingJobManagerRunner registerTestingJobManagerRunner() {
        final TestingJobManagerRunner jobManagerRunner =
                TestingJobManagerRunner.newBuilder().build();
        testInstance.register(jobManagerRunner);

        assertThat(testInstance.isRegistered(jobManagerRunner.getJobID())).isTrue();
        assertThat(jobManagerRunner.getTerminationFuture()).isNotDone();

        return jobManagerRunner;
    }
}
