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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.execution.librarycache.TestingClassLoaderLease;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.highavailability.JobResultEntry;
import org.apache.flink.runtime.highavailability.JobResultStore;
import org.apache.flink.runtime.highavailability.nonha.embedded.EmbeddedJobResultStore;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmaster.factories.JobMasterServiceProcessFactory;
import org.apache.flink.runtime.jobmaster.factories.TestingJobMasterServiceProcessFactory;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGatewayBuilder;
import org.apache.flink.runtime.leaderelection.DefaultLeaderElectionService;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.leaderelection.TestingLeaderElectionDriver;
import org.apache.flink.runtime.leaderelection.TestingLeaderElectionService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.runtime.testutils.TestingJobResultStore;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TestLoggerExtension;
import org.apache.flink.util.function.ThrowingRunnable;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.annotation.Nonnull;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link JobMasterServiceLeadershipRunner}. */
@ExtendWith(TestLoggerExtension.class)
class JobMasterServiceLeadershipRunnerTest {

    private static final Time TESTING_TIMEOUT = Time.seconds(10);

    private static JobGraph jobGraph;

    private TestingLeaderElectionService leaderElectionService;

    private TestingFatalErrorHandler fatalErrorHandler;

    private JobResultStore jobResultStore;

    @BeforeAll
    static void setupClass() {

        final JobVertex jobVertex = new JobVertex("Test vertex");
        jobVertex.setInvokableClass(NoOpInvokable.class);
        jobGraph = JobGraphTestUtils.streamingJobGraph(jobVertex);
    }

    @BeforeEach
    void setup() {
        leaderElectionService = new TestingLeaderElectionService();
        jobResultStore = new EmbeddedJobResultStore();
        fatalErrorHandler = new TestingFatalErrorHandler();
    }

    @AfterEach
    void tearDown() throws Exception {
        fatalErrorHandler.rethrowError();
    }

    @Test
    void testShutDownSignalsJobAsNotFinished() throws Exception {
        try (JobManagerRunner jobManagerRunner =
                newJobMasterServiceLeadershipRunnerBuilder().build()) {
            jobManagerRunner.start();

            final CompletableFuture<JobManagerRunnerResult> resultFuture =
                    jobManagerRunner.getResultFuture();

            assertThat(resultFuture).isNotDone();

            jobManagerRunner.closeAsync();

            assertJobNotFinished(resultFuture);
            assertThat(jobManagerRunner.getJobMasterGateway())
                    .failsWithin(5L, TimeUnit.MILLISECONDS);
        }
    }

    @Test
    void testCloseReleasesClassLoaderLease() throws Exception {
        final OneShotLatch closeClassLoaderLeaseLatch = new OneShotLatch();

        final TestingClassLoaderLease classLoaderLease =
                TestingClassLoaderLease.newBuilder()
                        .setCloseRunnable(closeClassLoaderLeaseLatch::trigger)
                        .build();

        try (JobManagerRunner jobManagerRunner =
                newJobMasterServiceLeadershipRunnerBuilder()
                        .setClassLoaderLease(classLoaderLease)
                        .build()) {
            jobManagerRunner.start();

            jobManagerRunner.close();

            closeClassLoaderLeaseLatch.await();
        }
    }

    /**
     * Tests that the {@link JobMasterServiceLeadershipRunner} always waits for the previous
     * leadership operation (granting or revoking leadership) to finish before starting a new
     * leadership operation.
     */
    @Test
    void testConcurrentLeadershipOperationsBlockingClose() throws Exception {
        final CompletableFuture<Void> terminationFuture = new CompletableFuture<>();

        final JobManagerRunner jobManagerRunner =
                newJobMasterServiceLeadershipRunnerBuilder()
                        .withJobMasterServiceProcesses(
                                TestingJobMasterServiceProcess.newBuilder()
                                        .setTerminationFuture(terminationFuture)
                                        .withManualTerminationFutureCompletion()
                                        .build(),
                                TestingJobMasterServiceProcess.newBuilder().build())
                        .build();

        jobManagerRunner.start();

        leaderElectionService.isLeader(UUID.randomUUID()).get();

        leaderElectionService.notLeader();

        final CompletableFuture<UUID> leaderFuture =
                leaderElectionService.isLeader(UUID.randomUUID());

        // the new leadership should wait first for the suspension to happen
        assertThat(leaderFuture).isNotDone();

        assertThat(leaderFuture)
                .withFailMessage(
                        "Granted leadership even though the JobMaster has not been suspended.")
                .failsWithin(1L, TimeUnit.MILLISECONDS);

        terminationFuture.complete(null);

        leaderFuture.get();
    }

    @Test
    void testExceptionallyCompletedResultFutureFromJobMasterServiceProcessIsForwarded()
            throws Exception {
        final CompletableFuture<JobManagerRunnerResult> resultFuture = new CompletableFuture<>();
        final TestingJobMasterServiceProcess testingJobMasterServiceProcess =
                TestingJobMasterServiceProcess.newBuilder()
                        .setJobManagerRunnerResultFuture(resultFuture)
                        .build();

        JobManagerRunner jobManagerRunner =
                newJobMasterServiceLeadershipRunnerBuilder()
                        .withSingleJobMasterServiceProcess(testingJobMasterServiceProcess)
                        .build();

        jobManagerRunner.start();

        leaderElectionService.isLeader(UUID.randomUUID()).get();

        final FlinkException cause =
                new FlinkException("The JobMasterService failed unexpectedly.");
        resultFuture.completeExceptionally(cause);

        assertThat(jobManagerRunner.getResultFuture())
                .failsWithin(5L, TimeUnit.MILLISECONDS)
                .withThrowableOfType(ExecutionException.class)
                .withCause(cause);
    }

    @Test
    void testJobMasterCreationFailureCompletesJobManagerRunnerWithInitializationError()
            throws Exception {

        final FlinkException testException = new FlinkException("Test exception");

        final CompletableFuture<JobManagerRunnerResult> completedResultFuture =
                CompletableFuture.completedFuture(
                        JobManagerRunnerResult.forInitializationFailure(
                                createFailedExecutionGraphInfo(testException), testException));
        final JobMasterServiceLeadershipRunner jobManagerRunner =
                newJobMasterServiceLeadershipRunnerBuilder()
                        .withSingleJobMasterServiceProcess(
                                TestingJobMasterServiceProcess.newBuilder()
                                        .setJobManagerRunnerResultFuture(completedResultFuture)
                                        .build())
                        .build();

        jobManagerRunner.start();

        leaderElectionService.isLeader(UUID.randomUUID());

        final JobManagerRunnerResult jobManagerRunnerResult =
                jobManagerRunner.getResultFuture().join();
        assertThat(jobManagerRunnerResult.isInitializationFailure()).isTrue();

        assertThat(jobManagerRunnerResult.getInitializationFailure()).isEqualTo(testException);
    }

    @Nonnull
    private ExecutionGraphInfo createFailedExecutionGraphInfo(FlinkException testException) {
        return new ExecutionGraphInfo(
                ArchivedExecutionGraph.createSparseArchivedExecutionGraph(
                        jobGraph.getJobID(),
                        jobGraph.getName(),
                        JobStatus.FAILED,
                        testException,
                        null,
                        1L));
    }

    @Test
    void testJobMasterServiceProcessIsTerminatedOnClose() throws Exception {
        final CompletableFuture<Void> terminationFuture = new CompletableFuture<>();

        final JobManagerRunner jobManagerRunner =
                newJobMasterServiceLeadershipRunnerBuilder()
                        .withSingleJobMasterServiceProcess(
                                TestingJobMasterServiceProcess.newBuilder()
                                        .setTerminationFuture(terminationFuture)
                                        .build())
                        .build();

        jobManagerRunner.start();

        leaderElectionService.isLeader(UUID.randomUUID());

        assertJobManagerRunnerStarted();

        jobManagerRunner.closeAsync().join();

        assertJobNotFinished(jobManagerRunner.getResultFuture());
        assertThat(terminationFuture).isDone();
    }

    @Test
    void testJobMasterServiceProcessShutdownOnLeadershipLoss() throws Exception {
        final CompletableFuture<Void> terminationFuture = new CompletableFuture<>();
        final JobManagerRunner jobManagerRunner =
                newJobMasterServiceLeadershipRunnerBuilder()
                        .withSingleJobMasterServiceProcess(
                                TestingJobMasterServiceProcess.newBuilder()
                                        .setTerminationFuture(terminationFuture)
                                        .build())
                        .build();

        jobManagerRunner.start();

        leaderElectionService.isLeader(UUID.randomUUID());
        assertJobManagerRunnerStarted();

        leaderElectionService.notLeader();

        assertThat(terminationFuture)
                .succeedsWithin(TESTING_TIMEOUT.getSize(), TESTING_TIMEOUT.getUnit());
    }

    @Test
    void testCancellationIsForwardedToJobMasterService() throws Exception {
        final CompletableFuture<JobMasterGateway> jobMasterGatewayFuture =
                new CompletableFuture<>();
        final JobManagerRunner jobManagerRunner =
                newJobMasterServiceLeadershipRunnerBuilder()
                        .withSingleJobMasterServiceProcess(
                                TestingJobMasterServiceProcess.newBuilder()
                                        .setJobMasterGatewayFuture(jobMasterGatewayFuture)
                                        .build())
                        .build();

        jobManagerRunner.start();

        leaderElectionService.isLeader(UUID.randomUUID());

        // cancel during init
        CompletableFuture<Acknowledge> cancellationFuture =
                jobManagerRunner.cancel(TESTING_TIMEOUT);

        assertThat(cancellationFuture).isNotDone();

        AtomicBoolean cancelCalled = new AtomicBoolean(false);
        JobMasterGateway jobMasterGateway =
                new TestingJobMasterGatewayBuilder()
                        .setCancelFunction(
                                () -> {
                                    cancelCalled.set(true);
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                })
                        .build();

        jobMasterGatewayFuture.complete(jobMasterGateway);

        // assert that cancellation future completes when cancellation completes.
        cancellationFuture.get();
        assertThat(cancelCalled).isTrue();
    }

    @Test
    void testJobInformationOperationsDuringInitialization() throws Exception {

        final JobManagerRunner jobManagerRunner =
                newJobMasterServiceLeadershipRunnerBuilder()
                        .withSingleJobMasterServiceProcess(
                                TestingJobMasterServiceProcess.newBuilder()
                                        .setIsInitialized(false)
                                        .build())
                        .build();

        jobManagerRunner.start();

        // assert initializing while waiting for leadership
        assertInitializingStates(jobManagerRunner);

        // assign leadership
        leaderElectionService.isLeader(UUID.randomUUID());

        // assert initializing while not yet initialized
        assertInitializingStates(jobManagerRunner);
    }

    private static void assertInitializingStates(JobManagerRunner jobManagerRunner)
            throws ExecutionException, InterruptedException {
        assertThat(jobManagerRunner.requestJobStatus(TESTING_TIMEOUT).get())
                .isEqualTo(JobStatus.INITIALIZING);
        assertThat(jobManagerRunner.getResultFuture()).isNotDone();
        assertThat(
                        jobManagerRunner
                                .requestJob(TESTING_TIMEOUT)
                                .get()
                                .getArchivedExecutionGraph()
                                .getState())
                .isEqualTo(JobStatus.INITIALIZING);

        assertThat(jobManagerRunner.requestJobDetails(TESTING_TIMEOUT).get().getStatus())
                .isEqualTo(JobStatus.INITIALIZING);
    }

    // It can happen that a series of leadership operations happens while the JobMaster
    // initialization is blocked. This test is to ensure that we are not starting-stopping
    // JobMasters for all pending leadership grants, but only for the latest.
    @Test
    void testSkippingOfEnqueuedLeadershipOperations() throws Exception {
        final CompletableFuture<Void> firstTerminationFuture = new CompletableFuture<>();
        final CompletableFuture<Void> secondTerminationFuture = new CompletableFuture<>();

        final JobManagerRunner jobManagerRunner =
                newJobMasterServiceLeadershipRunnerBuilder()
                        .withJobMasterServiceProcesses(
                                TestingJobMasterServiceProcess.newBuilder()
                                        .setTerminationFuture(firstTerminationFuture)
                                        .withManualTerminationFutureCompletion()
                                        .setIsInitialized(false)
                                        .build(),
                                TestingJobMasterServiceProcess.newBuilder()
                                        .setTerminationFuture(secondTerminationFuture)
                                        .build())
                        .build();

        jobManagerRunner.start();

        // first leadership assignment to get into blocking initialization
        leaderElectionService.isLeader(UUID.randomUUID());

        assertJobManagerRunnerStarted();

        assertThat(jobManagerRunner.requestJobStatus(TESTING_TIMEOUT).get())
                .isEqualTo(JobStatus.INITIALIZING);

        // we are now blocked on the initialization, enqueue some operations:
        for (int i = 0; i < 10; i++) {
            leaderElectionService.notLeader();
            leaderElectionService.isLeader(UUID.randomUUID());
        }

        firstTerminationFuture.complete(null);

        assertJobManagerRunnerStarted();
        jobManagerRunner.closeAsync();

        // this ensures that the second JobMasterServiceProcess is taken
        assertThat(secondTerminationFuture)
                .succeedsWithin(TESTING_TIMEOUT.getSize(), TESTING_TIMEOUT.getUnit());
    }

    @Test
    void testCancellationFailsWhenInitializationFails() throws Exception {
        final FlinkException testException = new FlinkException("test exception");
        runCancellationFailsTest(
                resultFuture ->
                        resultFuture.complete(
                                JobManagerRunnerResult.forInitializationFailure(
                                        createFailedExecutionGraphInfo(testException),
                                        testException)));
    }

    @Test
    void testCancellationFailsWhenExceptionOccurs() throws Exception {
        final FlinkException testException = new FlinkException("test exception");
        runCancellationFailsTest(resultFuture -> resultFuture.completeExceptionally(testException));
    }

    void runCancellationFailsTest(Consumer<CompletableFuture<JobManagerRunnerResult>> testAction)
            throws Exception {
        final CompletableFuture<JobManagerRunnerResult> jobManagerRunnerResultFuture =
                new CompletableFuture<>();
        final JobManagerRunner jobManagerRunner =
                newJobMasterServiceLeadershipRunnerBuilder()
                        .withSingleJobMasterServiceProcess(
                                TestingJobMasterServiceProcess.newBuilder()
                                        .setIsInitialized(false)
                                        .setJobMasterGatewayFuture(new CompletableFuture<>())
                                        .setJobManagerRunnerResultFuture(
                                                jobManagerRunnerResultFuture)
                                        .build())
                        .build();

        jobManagerRunner.start();

        leaderElectionService.isLeader(UUID.randomUUID());

        // cancel while initializing
        assertThat(jobManagerRunner.requestJobStatus(TESTING_TIMEOUT).get())
                .isEqualTo(JobStatus.INITIALIZING);

        CompletableFuture<Acknowledge> cancelFuture = jobManagerRunner.cancel(TESTING_TIMEOUT);
        assertThat(cancelFuture).isNotDone();

        testAction.accept(jobManagerRunnerResultFuture);
        assertThatThrownBy(cancelFuture::get).hasMessageContaining("Cancellation failed.");
    }

    @Test
    void testResultFutureCompletionOfOutdatedLeaderIsIgnored() throws Exception {
        final CompletableFuture<JobManagerRunnerResult> resultFuture = new CompletableFuture<>();
        final JobMasterServiceLeadershipRunner jobManagerRunner =
                newJobMasterServiceLeadershipRunnerBuilder()
                        .withSingleJobMasterServiceProcess(
                                TestingJobMasterServiceProcess.newBuilder()
                                        .setJobManagerRunnerResultFuture(resultFuture)
                                        .build())
                        .build();

        jobManagerRunner.start();

        leaderElectionService.isLeader(UUID.randomUUID()).join();

        leaderElectionService.notLeader();

        resultFuture.complete(
                JobManagerRunnerResult.forSuccess(
                        createFailedExecutionGraphInfo(new FlinkException("test exception"))));

        assertThat(jobManagerRunner.getResultFuture()).failsWithin(5L, TimeUnit.MILLISECONDS);
    }

    @Test
    void testJobMasterGatewayIsInvalidatedOnLeadershipChanges() throws Exception {
        final JobMasterServiceLeadershipRunner jobManagerRunner =
                newJobMasterServiceLeadershipRunnerBuilder()
                        .withSingleJobMasterServiceProcess(
                                TestingJobMasterServiceProcess.newBuilder()
                                        .setJobMasterGatewayFuture(new CompletableFuture<>())
                                        .build())
                        .build();

        jobManagerRunner.start();

        final CompletableFuture<JobMasterGateway> jobMasterGateway =
                jobManagerRunner.getJobMasterGateway();

        leaderElectionService.isLeader(UUID.randomUUID());

        leaderElectionService.notLeader();

        assertThat(jobMasterGateway).failsWithin(5L, TimeUnit.MILLISECONDS);
    }

    @Test
    void testLeaderAddressOfOutdatedLeaderIsIgnored() throws Exception {
        final CompletableFuture<String> leaderAddressFuture = new CompletableFuture<>();
        final JobMasterServiceLeadershipRunner jobManagerRunner =
                newJobMasterServiceLeadershipRunnerBuilder()
                        .withSingleJobMasterServiceProcess(
                                TestingJobMasterServiceProcess.newBuilder()
                                        .setLeaderAddressFuture(leaderAddressFuture)
                                        .build())
                        .build();

        jobManagerRunner.start();

        final CompletableFuture<UUID> leaderFuture =
                leaderElectionService.isLeader(UUID.randomUUID());

        leaderElectionService.notLeader();

        leaderAddressFuture.complete("foobar");

        assertThat(leaderFuture).failsWithin(5L, TimeUnit.MILLISECONDS);
    }

    @Test
    void testInitialJobStatusIsInitializing() throws Exception {
        final JobMasterServiceLeadershipRunner jobManagerRunner =
                newJobMasterServiceLeadershipRunnerBuilder().build();

        jobManagerRunner.start();

        assertThat(jobManagerRunner.requestJobStatus(TESTING_TIMEOUT).join())
                .isEqualTo(JobStatus.INITIALIZING);
    }

    @Test
    void testCancellationChangesJobStatusToCancelling() throws Exception {
        final JobMasterServiceLeadershipRunner jobManagerRunner =
                newJobMasterServiceLeadershipRunnerBuilder().build();

        jobManagerRunner.start();

        jobManagerRunner.cancel(TESTING_TIMEOUT);

        assertThat(jobManagerRunner.requestJobStatus(TESTING_TIMEOUT).join())
                .isEqualTo(JobStatus.CANCELLING);
    }

    @Test
    void testJobStatusCancellingIsClearedOnLeadershipLoss() throws Exception {
        CompletableFuture<Void> terminationFuture = new CompletableFuture<>();
        final JobMasterServiceLeadershipRunner jobManagerRunner =
                newJobMasterServiceLeadershipRunnerBuilder()
                        .withSingleJobMasterServiceProcess(
                                TestingJobMasterServiceProcess.newBuilder()
                                        .setTerminationFuture(terminationFuture)
                                        .build())
                        .build();

        jobManagerRunner.start();

        jobManagerRunner.cancel(TESTING_TIMEOUT);

        leaderElectionService.isLeader(UUID.randomUUID());
        assertJobManagerRunnerStarted();

        leaderElectionService.notLeader();
        assertThat(terminationFuture)
                .succeedsWithin(TESTING_TIMEOUT.getSize(), TESTING_TIMEOUT.getUnit());

        assertThat(jobManagerRunner.requestJobStatus(TESTING_TIMEOUT).join())
                .isEqualTo(JobStatus.INITIALIZING);
    }

    @Test
    void testJobMasterServiceProcessClosingExceptionIsForwardedToResultFuture() throws Exception {
        final CompletableFuture<Void> terminationFuture = new CompletableFuture<>();
        final JobMasterServiceLeadershipRunner jobManagerRunner =
                newJobMasterServiceLeadershipRunnerBuilder()
                        .withSingleJobMasterServiceProcess(
                                TestingJobMasterServiceProcess.newBuilder()
                                        .setTerminationFuture(terminationFuture)
                                        .withManualTerminationFutureCompletion()
                                        .build())
                        .build();

        jobManagerRunner.start();

        leaderElectionService.isLeader(UUID.randomUUID());
        assertJobManagerRunnerStarted();

        leaderElectionService.notLeader();

        final FlinkException testException = new FlinkException("Test exception");
        terminationFuture.completeExceptionally(testException);

        assertThat(jobManagerRunner.getResultFuture())
                .failsWithin(5L, TimeUnit.MILLISECONDS)
                .withThrowableOfType(ExecutionException.class)
                .satisfies(cause -> assertThat(cause).hasRootCause(testException));
    }

    @Test
    void testJobMasterServiceProcessCreationFailureIsForwardedToResultFuture() throws Exception {
        final FlinkRuntimeException testException = new FlinkRuntimeException("Test exception");
        final JobMasterServiceLeadershipRunner jobManagerRunner =
                newJobMasterServiceLeadershipRunnerBuilder()
                        .setJobMasterServiceProcessFactory(
                                TestingJobMasterServiceProcessFactory.newBuilder()
                                        .setJobMasterServiceProcessFunction(
                                                ignored -> {
                                                    throw testException;
                                                })
                                        .build())
                        .build();

        jobManagerRunner.start();

        leaderElectionService.isLeader(UUID.randomUUID());

        assertThat(jobManagerRunner.getResultFuture())
                .failsWithin(5L, TimeUnit.MILLISECONDS)
                .withThrowableOfType(ExecutionException.class)
                .satisfies(cause -> assertThat(cause).hasRootCause(testException));
    }

    @Test
    void testJobAlreadyDone() throws Exception {
        final JobID jobId = new JobID();
        final JobResult jobResult =
                TestingJobResultStore.createJobResult(jobId, ApplicationStatus.UNKNOWN);
        jobResultStore.createDirtyResult(new JobResultEntry(jobResult));
        try (JobManagerRunner jobManagerRunner =
                newJobMasterServiceLeadershipRunnerBuilder()
                        .setJobMasterServiceProcessFactory(
                                TestingJobMasterServiceProcessFactory.newBuilder()
                                        .setJobId(jobId)
                                        .build())
                        .build()) {
            jobManagerRunner.start();
            leaderElectionService.isLeader(UUID.randomUUID());

            final CompletableFuture<JobManagerRunnerResult> resultFuture =
                    jobManagerRunner.getResultFuture();

            JobManagerRunnerResult result = resultFuture.get();
            assertThat(result.getExecutionGraphInfo().getArchivedExecutionGraph().getState())
                    .isEqualTo(JobStatus.FAILED);
        }
    }

    @Test
    void testJobMasterServiceLeadershipRunnerCloseWhenElectionServiceGrantLeaderShip()
            throws Exception {
        final TestingLeaderElectionDriver.TestingLeaderElectionDriverFactory
                testingLeaderElectionDriverFactory =
                        new TestingLeaderElectionDriver.TestingLeaderElectionDriverFactory();

        // we need to use DefaultLeaderElectionService here because JobMasterServiceLeadershipRunner
        // in connection with the DefaultLeaderElectionService generates the nested locking
        final LeaderElectionService defaultLeaderElectionService =
                new DefaultLeaderElectionService(testingLeaderElectionDriverFactory);

        // latch to detect when we reached the first synchronized section having a lock on the
        // JobMasterServiceProcess#stop side
        final OneShotLatch closeAsyncCalledTrigger = new OneShotLatch();
        // latch to halt the JobMasterServiceProcess#stop before calling stop on the
        // DefaultLeaderElectionService instance (and entering the LeaderElectionService's
        // synchronized block)
        final OneShotLatch triggerClassLoaderLeaseRelease = new OneShotLatch();

        final JobMasterServiceProcess jobMasterServiceProcess =
                TestingJobMasterServiceProcess2.newBuilder()
                        .setGetJobMasterGatewayFutureSupplier(CompletableFuture::new)
                        .setGetResultFutureSupplier(CompletableFuture::new)
                        .setGetLeaderAddressFutureSupplier(
                                () -> CompletableFuture.completedFuture("unused address"))
                        .setCloseAsyncSupplier(
                                () -> {
                                    closeAsyncCalledTrigger.trigger();
                                    // we have to return a completed future because we need the
                                    // follow-up task to run in the calling thread to make the
                                    // follow-up logic block be executed in the synchronized block
                                    return CompletableFuture.completedFuture(null);
                                })
                        .build();
        try (final JobMasterServiceLeadershipRunner jobManagerRunner =
                newJobMasterServiceLeadershipRunnerBuilder()
                        .setClassLoaderLease(
                                TestingClassLoaderLease.newBuilder()
                                        .setCloseRunnable(
                                                () -> {
                                                    try {
                                                        // we want to wait with releasing to halt
                                                        // before calling stop on the
                                                        // DefaultLeaderElectioNService
                                                        triggerClassLoaderLeaseRelease.await();
                                                    } catch (InterruptedException e) {
                                                        ExceptionUtils.checkInterrupted(e);
                                                    }
                                                })
                                        .build())
                        .setJobMasterServiceProcessFactory(
                                TestingJobMasterServiceProcessFactory.newBuilder()
                                        .setJobMasterServiceProcessFunction(
                                                ignoredSessionId -> jobMasterServiceProcess)
                                        .build())
                        .setLeaderElectionService(defaultLeaderElectionService)
                        .build()) {
            jobManagerRunner.start();

            final TestingLeaderElectionDriver currentLeaderDriver =
                    Preconditions.checkNotNull(
                            testingLeaderElectionDriverFactory.getCurrentLeaderDriver());
            // grant leadership to create jobMasterServiceProcess
            currentLeaderDriver.isLeader();

            while (currentLeaderDriver.getLeaderInformation().getLeaderSessionID() == null
                    || !defaultLeaderElectionService.hasLeadership(
                            currentLeaderDriver.getLeaderInformation().getLeaderSessionID())) {
                Thread.sleep(100);
            }

            final CheckedThread contenderCloseThread = createCheckedThread(jobManagerRunner::close);
            contenderCloseThread.start();

            // waiting for the contender reaching the synchronized section of the stop call
            closeAsyncCalledTrigger.await();

            final CheckedThread grantLeadershipThread =
                    createCheckedThread(currentLeaderDriver::isLeader);
            grantLeadershipThread.start();

            // finalize ClassloaderLease release to trigger DefaultLeaderElectionService#stop
            triggerClassLoaderLeaseRelease.trigger();

            contenderCloseThread.sync();
            grantLeadershipThread.sync();
        }
    }

    private static CheckedThread createCheckedThread(
            ThrowingRunnable<? extends Exception> callback) {
        return new CheckedThread() {
            @Override
            public void go() throws Exception {
                callback.run();
            }
        };
    }

    private void assertJobManagerRunnerStarted() throws Exception {
        leaderElectionService
                .getConfirmationFuture()
                .get(TESTING_TIMEOUT.getSize(), TESTING_TIMEOUT.getUnit());
    }

    private void assertJobNotFinished(CompletableFuture<JobManagerRunnerResult> resultFuture)
            throws ExecutionException, InterruptedException {
        final JobManagerRunnerResult jobManagerRunnerResult = resultFuture.get();
        assertThat(
                        jobManagerRunnerResult
                                .getExecutionGraphInfo()
                                .getArchivedExecutionGraph()
                                .getState())
                .isEqualTo(JobStatus.SUSPENDED);
    }

    public JobMasterServiceLeadershipRunnerBuilder newJobMasterServiceLeadershipRunnerBuilder() {
        return new JobMasterServiceLeadershipRunnerBuilder();
    }

    public class JobMasterServiceLeadershipRunnerBuilder {
        private JobMasterServiceProcessFactory jobMasterServiceProcessFactory =
                TestingJobMasterServiceProcessFactory.newBuilder().build();
        private LibraryCacheManager.ClassLoaderLease classLoaderLease =
                TestingClassLoaderLease.newBuilder().build();

        private LeaderElectionService leaderElectionService =
                JobMasterServiceLeadershipRunnerTest.this.leaderElectionService;

        public JobMasterServiceLeadershipRunnerBuilder setClassLoaderLease(
                LibraryCacheManager.ClassLoaderLease classLoaderLease) {
            this.classLoaderLease = classLoaderLease;
            return this;
        }

        public JobMasterServiceLeadershipRunnerBuilder setJobMasterServiceProcessFactory(
                JobMasterServiceProcessFactory jobMasterServiceProcessFactory) {
            this.jobMasterServiceProcessFactory = jobMasterServiceProcessFactory;
            return this;
        }

        public JobMasterServiceLeadershipRunnerBuilder setLeaderElectionService(
                LeaderElectionService leaderElectionService) {
            this.leaderElectionService = leaderElectionService;
            return this;
        }

        public JobMasterServiceLeadershipRunner build() {
            return new JobMasterServiceLeadershipRunner(
                    jobMasterServiceProcessFactory,
                    leaderElectionService,
                    jobResultStore,
                    classLoaderLease,
                    fatalErrorHandler);
        }

        public JobMasterServiceLeadershipRunnerBuilder withSingleJobMasterServiceProcess(
                JobMasterServiceProcess jobMasterServiceProcess) {
            return withJobMasterServiceProcesses(jobMasterServiceProcess);
        }

        public JobMasterServiceLeadershipRunnerBuilder withJobMasterServiceProcesses(
                JobMasterServiceProcess... jobMasterServiceProcesses) {
            final Queue<JobMasterServiceProcess> jobMasterServiceProcessQueue =
                    new ArrayDeque<>(Arrays.asList(jobMasterServiceProcesses));
            this.jobMasterServiceProcessFactory =
                    TestingJobMasterServiceProcessFactory.newBuilder()
                            .setJobMasterServiceProcessFunction(
                                    ignored ->
                                            Preconditions.checkNotNull(
                                                    jobMasterServiceProcessQueue.poll()))
                            .build();
            return this;
        }
    }
}
