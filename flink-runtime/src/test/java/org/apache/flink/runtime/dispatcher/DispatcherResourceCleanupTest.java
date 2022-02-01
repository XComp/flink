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
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.FlinkMatchers;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.BlobUtils;
import org.apache.flink.runtime.blob.TestingBlobStoreBuilder;
import org.apache.flink.runtime.client.DuplicateJobSubmissionException;
import org.apache.flink.runtime.client.JobSubmissionException;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.dispatcher.cleanup.TestingResourceCleanerFactory;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.JobResultEntry;
import org.apache.flink.runtime.highavailability.JobResultStore;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobmaster.JobManagerRunner;
import org.apache.flink.runtime.jobmaster.JobManagerSharedServices;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.jobmaster.TestingJobManagerRunner;
import org.apache.flink.runtime.jobmaster.factories.JobManagerJobMetricGroupFactory;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGateway;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGatewayBuilder;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedExecutionGraphBuilder;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.runtime.testutils.TestingJobResultStore;
import org.apache.flink.runtime.util.TestingFatalErrorHandlerResource;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.Reference;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.ThrowingRunnable;

import org.hamcrest.core.IsInstanceOf;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.core.testutils.FlinkMatchers.containsCause;
import static org.apache.flink.core.testutils.FlinkMatchers.containsMessage;
import static org.apache.flink.runtime.dispatcher.AbstractDispatcherTest.awaitStatus;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests the resource cleanup by the {@link Dispatcher}. */
public class DispatcherResourceCleanupTest extends TestLogger {

    @ClassRule public static TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Rule public ExpectedException expectedException = ExpectedException.none();

    @Rule
    public final TestingFatalErrorHandlerResource testingFatalErrorHandlerResource =
            new TestingFatalErrorHandlerResource();

    private static final Time timeout = Time.seconds(10L);

    private static TestingRpcService rpcService;

    private JobID jobId;

    private JobGraph jobGraph;

    private JobResultStore jobResultStore;

    private OneShotLatch clearedJobLatch;

    private TestingDispatcher dispatcher;

    private DispatcherGateway dispatcherGateway;

    private BlobServer blobServer;

    private CompletableFuture<JobID> localCleanupFuture;
    private CompletableFuture<JobID> globalCleanupFuture;

    @BeforeClass
    public static void setupClass() {
        rpcService = new TestingRpcService();
    }

    @Before
    public void setup() throws Exception {
        jobGraph = JobGraphTestUtils.singleNoOpJobGraph();
        jobId = jobGraph.getJobID();

        clearedJobLatch = new OneShotLatch();
        jobResultStore = new SingleJobResultStore(jobId, clearedJobLatch);

        globalCleanupFuture = new CompletableFuture<>();
        localCleanupFuture = new CompletableFuture<>();

        blobServer =
                BlobUtils.createBlobServer(
                        new Configuration(),
                        Reference.owned(temporaryFolder.newFolder()),
                        new TestingBlobStoreBuilder().createTestingBlobStore());
    }

    private TestingJobManagerRunnerFactory startDispatcherAndSubmitJob() throws Exception {
        return startDispatcherAndSubmitJob(0);
    }

    private TestingJobManagerRunnerFactory startDispatcherAndSubmitJob(
            int numBlockingJobManagerRunners) throws Exception {
        final TestingJobManagerRunnerFactory testingJobManagerRunnerFactoryNG =
                new TestingJobManagerRunnerFactory(numBlockingJobManagerRunners);
        startDispatcher(testingJobManagerRunnerFactoryNG);
        submitJobAndWait();

        return testingJobManagerRunnerFactoryNG;
    }

    private void startDispatcher(JobManagerRunnerFactory jobManagerRunnerFactory) throws Exception {
        final JobManagerRunnerRegistry jobManagerRunnerRegistry =
                new DefaultJobManagerRunnerRegistry(2);
        dispatcher =
                TestingDispatcher.builder()
                        .withRpcService(rpcService)
                        .withJobResultStore(jobResultStore)
                        .withJobManagerRunnerRegistry(jobManagerRunnerRegistry)
                        .withBlobServer(blobServer)
                        .withFatalErrorHandler(
                                testingFatalErrorHandlerResource.getFatalErrorHandler())
                        .withJobManagerRunnerFactory(jobManagerRunnerFactory)
                        .withResourceCleanerFactory(
                                new TestingResourceCleanerFactory()
                                        // JobManagerRunnerRegistry needs to be added explicitly
                                        // because cleaning it will trigger the closeAsync latch
                                        // provided by TestingJobManagerRunner
                                        .withLocallyCleanableResource(jobManagerRunnerRegistry)
                                        .withGloballyCleanableResource(
                                                globalCleanupFuture::complete)
                                        .withLocallyCleanableResource(localCleanupFuture::complete))
                        .build();

        dispatcher.start();

        dispatcherGateway = dispatcher.getSelfGateway(DispatcherGateway.class);
    }

    @After
    public void teardown() throws Exception {
        if (dispatcher != null) {
            dispatcher.close();
        }

        if (blobServer != null) {
            blobServer.close();
        }
    }

    @AfterClass
    public static void teardownClass() throws ExecutionException, InterruptedException {
        if (rpcService != null) {
            rpcService.stopService().get();
        }
    }

    @Test
    public void testGlobalCleanupWhenJobFinished() throws Exception {
        final TestingJobManagerRunnerFactory jobManagerRunnerFactory =
                startDispatcherAndSubmitJob();

        // complete the job
        finishJob(jobManagerRunnerFactory.takeCreatedJobManagerRunner());

        assertGlobalCleanupTriggered(jobId);
    }

    private CompletableFuture<Acknowledge> submitJob() {
        return dispatcherGateway.submitJob(jobGraph, timeout);
    }

    private void submitJobAndWait() {
        submitJob().join();
    }

    @Test
    public void testLocalCleanupWhenJobNotFinished() throws Exception {
        final TestingJobManagerRunnerFactory jobManagerRunnerFactory =
                startDispatcherAndSubmitJob();

        // job not finished
        final TestingJobManagerRunner testingJobManagerRunner =
                jobManagerRunnerFactory.takeCreatedJobManagerRunner();
        suspendJob(testingJobManagerRunner);

        assertLocalCleanupTriggered(jobId);
    }

    @Test
    public void testGlobalCleanupWhenJobSubmissionFails() throws Exception {
        startDispatcher(new FailingJobManagerRunnerFactory(new FlinkException("Test exception")));
        final CompletableFuture<Acknowledge> submissionFuture = submitJob();

        try {
            submissionFuture.get();
            fail("Job submission was expected to fail.");
        } catch (ExecutionException ee) {
            assertThat(ee, FlinkMatchers.containsCause(JobSubmissionException.class));
        }

        assertGlobalCleanupTriggered(jobId);
    }

    @Test
    public void testLocalCleanupWhenClosingDispatcher() throws Exception {
        startDispatcherAndSubmitJob();

        dispatcher.closeAsync().get();

        assertLocalCleanupTriggered(jobId);
    }

    @Test
    public void testGlobalCleanupWhenJobFinishedWhileClosingDispatcher() throws Exception {
        final TestingJobManagerRunner testingJobManagerRunner =
                TestingJobManagerRunner.newBuilder()
                        .setBlockingTermination(true)
                        .setJobId(jobId)
                        .build();

        final Queue<JobManagerRunner> jobManagerRunners =
                new ArrayDeque<>(Arrays.asList(testingJobManagerRunner));

        startDispatcher(new QueueJobManagerRunnerFactory(jobManagerRunners));
        submitJobAndWait();

        final CompletableFuture<Void> dispatcherTerminationFuture = dispatcher.closeAsync();

        testingJobManagerRunner.getCloseAsyncCalledLatch().await();
        testingJobManagerRunner.completeResultFuture(
                new ExecutionGraphInfo(
                        new ArchivedExecutionGraphBuilder()
                                .setJobID(jobId)
                                .setState(JobStatus.FINISHED)
                                .build()));

        testingJobManagerRunner.completeTerminationFuture();

        // check that no exceptions have been thrown
        dispatcherTerminationFuture.get();

        assertGlobalCleanupTriggered(jobId);
    }

    /**
     * Tests that the {@link JobResultStore} entries are marked as clean after the job reached a
     * terminal state.
     */
    @Test
    public void testJobResultStoreCleanup() throws Exception {
        final TestingJobManagerRunnerFactory jobManagerRunnerFactory =
                startDispatcherAndSubmitJob();

        final JobResult jobResult =
                TestingJobResultStore.createJobResult(jobId, ApplicationStatus.UNKNOWN);

        jobResultStore.createDirtyResult(new JobResultEntry(jobResult));
        assertTrue(jobResultStore.hasJobResultEntry(jobId));

        final TestingJobManagerRunner testingJobManagerRunner =
                jobManagerRunnerFactory.takeCreatedJobManagerRunner();
        testingJobManagerRunner.completeResultFuture(
                new ExecutionGraphInfo(
                        new ArchivedExecutionGraphBuilder()
                                .setState(JobStatus.FINISHED)
                                .setJobID(jobId)
                                .build()));

        // wait for the clearing
        clearedJobLatch.await();

        assertTrue(jobResultStore.hasJobResultEntry(jobId));
        assertTrue(jobResultStore.getDirtyResults().isEmpty());
    }

    /**
     * Tests that the previous JobManager needs to be completely terminated before a new job with
     * the same {@link JobID} is started.
     */
    @Test
    public void testJobSubmissionUnderSameJobId() throws Exception {
        final TestingJobManagerRunnerFactory jobManagerRunnerFactory =
                startDispatcherAndSubmitJob(1);

        final TestingJobManagerRunner testingJobManagerRunner =
                jobManagerRunnerFactory.takeCreatedJobManagerRunner();
        suspendJob(testingJobManagerRunner);

        // wait until termination JobManagerRunner closeAsync has been called.
        // this is necessary to avoid race conditions with completion of the 1st job and the
        // submission of the 2nd job (DuplicateJobSubmissionException).
        testingJobManagerRunner.getCloseAsyncCalledLatch().await();

        final CompletableFuture<Acknowledge> submissionFuture =
                dispatcherGateway.submitJob(jobGraph, timeout);

        try {
            submissionFuture.get(10L, TimeUnit.MILLISECONDS);
            fail(
                    "The job submission future should not complete until the previous JobManager "
                            + "termination future has been completed.");
        } catch (TimeoutException ignored) {
            // expected
        } finally {
            testingJobManagerRunner.completeTerminationFuture();
        }

        assertThat(submissionFuture.get(), equalTo(Acknowledge.get()));
    }

    /**
     * Tests that a duplicate job submission won't delete any job meta data (submitted job graphs,
     * blobs, etc.).
     */
    @Test
    public void testDuplicateJobSubmissionDoesNotDeleteJobMetaData() throws Exception {
        final TestingJobManagerRunnerFactory testingJobManagerRunnerFactoryNG =
                startDispatcherAndSubmitJob();

        final CompletableFuture<Acknowledge> submissionFuture =
                dispatcherGateway.submitJob(jobGraph, timeout);

        try {
            try {
                submissionFuture.get();
                fail("Expected a DuplicateJobSubmissionFailure.");
            } catch (ExecutionException ee) {
                assertThat(
                        ExceptionUtils.findThrowable(ee, DuplicateJobSubmissionException.class)
                                .isPresent(),
                        is(true));
            }

            assertThatNoCleanupWasTriggered();
        } finally {
            finishJob(testingJobManagerRunnerFactoryNG.takeCreatedJobManagerRunner());
        }

        assertGlobalCleanupTriggered(jobId);
    }

    private void finishJob(TestingJobManagerRunner takeCreatedJobManagerRunner) {
        terminateJobWithState(takeCreatedJobManagerRunner, JobStatus.FINISHED);
    }

    private void suspendJob(TestingJobManagerRunner takeCreatedJobManagerRunner) {
        terminateJobWithState(takeCreatedJobManagerRunner, JobStatus.SUSPENDED);
    }

    private void terminateJobWithState(
            TestingJobManagerRunner takeCreatedJobManagerRunner, JobStatus state) {
        takeCreatedJobManagerRunner.completeResultFuture(
                new ExecutionGraphInfo(
                        new ArchivedExecutionGraphBuilder()
                                .setJobID(jobId)
                                .setState(state)
                                .build()));
    }

    private void assertThatNoCleanupWasTriggered() {
        assertThat(globalCleanupFuture.isDone(), is(false));
        assertThat(localCleanupFuture.isDone(), is(false));
    }

    @Test
    public void testDispatcherTerminationTerminatesRunningJobMasters() throws Exception {
        final TestingJobManagerRunnerFactory jobManagerRunnerFactory =
                startDispatcherAndSubmitJob();

        dispatcher.closeAsync().get();

        final TestingJobManagerRunner jobManagerRunner =
                jobManagerRunnerFactory.takeCreatedJobManagerRunner();
        assertThat(jobManagerRunner.getTerminationFuture().isDone(), is(true));
    }

    /** Tests that terminating the Dispatcher will wait for all JobMasters to be terminated. */
    @Test
    public void testDispatcherTerminationWaitsForJobMasterTerminations() throws Exception {
        final TestingJobManagerRunnerFactory jobManagerRunnerFactory =
                startDispatcherAndSubmitJob(1);

        final CompletableFuture<Void> dispatcherTerminationFuture = dispatcher.closeAsync();

        try {
            dispatcherTerminationFuture.get(10L, TimeUnit.MILLISECONDS);
            fail("We should not terminate before all running JobMasters have terminated.");
        } catch (TimeoutException ignored) {
            // expected
        } finally {
            jobManagerRunnerFactory.takeCreatedJobManagerRunner().completeTerminationFuture();
        }

        dispatcherTerminationFuture.get();
    }

    private static final class SingleJobResultStore implements JobResultStore {

        private final JobID expectedJobId;
        @Nullable private JobResultEntry actualJobResultEntry;
        private boolean isDirty = true;

        private final OneShotLatch clearedJobLatch;

        private SingleJobResultStore(JobID expectedJobId, OneShotLatch clearedJobLatch) {
            this.expectedJobId = expectedJobId;
            this.clearedJobLatch = clearedJobLatch;
        }

        @Override
        public void createDirtyResult(JobResultEntry jobResultEntry) throws IOException {
            checkJobId(jobResultEntry.getJobId());
            this.actualJobResultEntry = jobResultEntry;
        }

        private void checkJobId(JobID jobID) {
            Preconditions.checkArgument(expectedJobId.equals(jobID));
        }

        @Override
        public void markResultAsClean(JobID jobId) throws IOException {
            checkJobId(jobId);
            Preconditions.checkNotNull(actualJobResultEntry);
            isDirty = false;
            clearedJobLatch.trigger();
        }

        @Override
        public boolean hasDirtyJobResultEntry(JobID jobId) throws IOException {
            if (actualJobResultEntry == null) {
                return false;
            }

            checkJobId(jobId);
            return isDirty;
        }

        @Override
        public boolean hasCleanJobResultEntry(JobID jobId) throws IOException {
            if (actualJobResultEntry == null) {
                return false;
            }

            checkJobId(jobId);
            return !isDirty;
        }

        @Override
        public Set<JobResult> getDirtyResults() throws IOException {
            return actualJobResultEntry != null && isDirty
                    ? Collections.singleton(actualJobResultEntry.getJobResult())
                    : Collections.emptySet();
        }
    }

    @Test
    public void testHABlobsAreRemovedIfHAJobGraphRemovalSucceeds() throws Exception {
        final TestingJobManagerRunnerFactory jobManagerRunnerFactory =
                startDispatcherAndSubmitJob();

        ArchivedExecutionGraph executionGraph =
                new ArchivedExecutionGraphBuilder()
                        .setJobID(jobId)
                        .setState(JobStatus.CANCELED)
                        .build();

        final TestingJobManagerRunner testingJobManagerRunner =
                jobManagerRunnerFactory.takeCreatedJobManagerRunner();
        testingJobManagerRunner.completeResultFuture(new ExecutionGraphInfo(executionGraph));

        assertGlobalCleanupTriggered(jobId);
    }

    private void assertLocalCleanupTriggered(JobID jobId)
            throws ExecutionException, InterruptedException, TimeoutException {
        assertThat(localCleanupFuture.get(100, TimeUnit.MILLISECONDS), equalTo(jobId));
        assertThat(globalCleanupFuture.isDone(), is(false));
    }

    private void assertGlobalCleanupTriggered(JobID jobId)
            throws ExecutionException, InterruptedException, TimeoutException {
        assertThat(localCleanupFuture.get(100, TimeUnit.MILLISECONDS), equalTo(jobId));
        assertThat(globalCleanupFuture.get(100, TimeUnit.MILLISECONDS), equalTo(jobId));
    }

    @Test
    public void testFatalErrorIfJobCannotBeMarkedDirtyInJobResultStore() throws Exception {
        jobResultStore =
                TestingJobResultStore.builder()
                        .withCreateDirtyResultConsumer(
                                jobResult -> {
                                    throw new IOException("Expected IOException.");
                                })
                        .build();

        final TestingJobManagerRunnerFactory jobManagerRunnerFactory =
                startDispatcherAndSubmitJob();

        ArchivedExecutionGraph executionGraph =
                new ArchivedExecutionGraphBuilder()
                        .setJobID(jobId)
                        .setState(JobStatus.FINISHED)
                        .build();

        final TestingJobManagerRunner testingJobManagerRunner =
                jobManagerRunnerFactory.takeCreatedJobManagerRunner();
        testingJobManagerRunner.completeResultFuture(new ExecutionGraphInfo(executionGraph));

        final CompletableFuture<? extends Throwable> errorFuture =
                this.testingFatalErrorHandlerResource.getFatalErrorHandler().getErrorFuture();
        assertThat(
                errorFuture.get(100, TimeUnit.MILLISECONDS),
                IsInstanceOf.instanceOf(FlinkException.class));
        testingFatalErrorHandlerResource.getFatalErrorHandler().clearError();
    }

    @Test
    public void testErrorHandlingIfJobCannotBeMarkedAsCleanInJobResultStore() throws Exception {
        final CompletableFuture<JobResultEntry> dirtyJobFuture = new CompletableFuture<>();
        jobResultStore =
                TestingJobResultStore.builder()
                        .withCreateDirtyResultConsumer(dirtyJobFuture::complete)
                        .withMarkResultAsCleanConsumer(
                                jobId -> {
                                    throw new IOException("Expected IOException.");
                                })
                        .build();

        final TestingJobManagerRunnerFactory jobManagerRunnerFactory =
                startDispatcherAndSubmitJob();

        ArchivedExecutionGraph executionGraph =
                new ArchivedExecutionGraphBuilder()
                        .setJobID(jobId)
                        .setState(JobStatus.FINISHED)
                        .build();

        final TestingJobManagerRunner testingJobManagerRunner =
                jobManagerRunnerFactory.takeCreatedJobManagerRunner();
        testingJobManagerRunner.completeResultFuture(new ExecutionGraphInfo(executionGraph));

        final CompletableFuture<? extends Throwable> errorFuture =
                this.testingFatalErrorHandlerResource.getFatalErrorHandler().getErrorFuture();
        try {
            final Throwable unexpectedError = errorFuture.get(100, TimeUnit.MILLISECONDS);
            fail(
                    "No error should have been reported but an "
                            + unexpectedError.getClass()
                            + " was handled.");
        } catch (TimeoutException e) {
            // expected
        }

        assertThat(dirtyJobFuture.get().getJobId(), is(jobId));
    }

    /** Tests that a failing {@link JobManagerRunner} will be properly cleaned up. */
    @Test
    public void testFailingJobManagerRunnerCleanup() throws Exception {
        final FlinkException testException = new FlinkException("Test exception.");
        final ArrayBlockingQueue<Optional<Exception>> queue = new ArrayBlockingQueue<>(2);

        final BlockingJobManagerRunnerFactory blockingJobManagerRunnerFactory =
                new BlockingJobManagerRunnerFactory(
                        () -> {
                            final Optional<Exception> maybeException = queue.take();
                            if (maybeException.isPresent()) {
                                throw maybeException.get();
                            }
                        });

        startDispatcher(blockingJobManagerRunnerFactory);

        final DispatcherGateway dispatcherGateway =
                dispatcher.getSelfGateway(DispatcherGateway.class);

        // submit and fail during job master runner construction
        queue.offer(Optional.of(testException));
        try {
            dispatcherGateway.submitJob(jobGraph, Time.minutes(1)).get();
            fail("A FlinkException is expected");
        } catch (Throwable expectedException) {
            assertThat(expectedException, containsCause(FlinkException.class));
            assertThat(expectedException, containsMessage(testException.getMessage()));
            // make sure we've cleaned up in correct order (including HA)
            assertGlobalCleanupTriggered(jobId);
        }

        // don't fail this time
        queue.offer(Optional.empty());
        // submit job again
        dispatcherGateway.submitJob(jobGraph, Time.minutes(1L)).get();
        blockingJobManagerRunnerFactory.setJobStatus(JobStatus.RUNNING);

        // Ensure job is running
        awaitStatus(dispatcherGateway, jobId, JobStatus.RUNNING);
    }

    private static final class BlockingJobManagerRunnerFactory
            extends TestingJobManagerRunnerFactory {

        @Nonnull private final ThrowingRunnable<Exception> jobManagerRunnerCreationLatch;
        private TestingJobManagerRunner testingRunner;

        BlockingJobManagerRunnerFactory(
                @Nonnull ThrowingRunnable<Exception> jobManagerRunnerCreationLatch) {
            this.jobManagerRunnerCreationLatch = jobManagerRunnerCreationLatch;
        }

        @Override
        public TestingJobManagerRunner createJobManagerRunner(
                JobGraph jobGraph,
                Configuration configuration,
                RpcService rpcService,
                HighAvailabilityServices highAvailabilityServices,
                HeartbeatServices heartbeatServices,
                JobManagerSharedServices jobManagerSharedServices,
                JobManagerJobMetricGroupFactory jobManagerJobMetricGroupFactory,
                FatalErrorHandler fatalErrorHandler,
                long initializationTimestamp)
                throws Exception {
            jobManagerRunnerCreationLatch.run();

            this.testingRunner =
                    super.createJobManagerRunner(
                            jobGraph,
                            configuration,
                            rpcService,
                            highAvailabilityServices,
                            heartbeatServices,
                            jobManagerSharedServices,
                            jobManagerJobMetricGroupFactory,
                            fatalErrorHandler,
                            initializationTimestamp);

            TestingJobMasterGateway testingJobMasterGateway =
                    new TestingJobMasterGatewayBuilder()
                            .setRequestJobSupplier(
                                    () ->
                                            CompletableFuture.completedFuture(
                                                    new ExecutionGraphInfo(
                                                            ArchivedExecutionGraph
                                                                    .createFromInitializingJob(
                                                                            jobGraph.getJobID(),
                                                                            jobGraph.getName(),
                                                                            JobStatus.RUNNING,
                                                                            null,
                                                                            null,
                                                                            1337))))
                            .build();
            testingRunner.completeJobMasterGatewayFuture(testingJobMasterGateway);
            return testingRunner;
        }

        public void setJobStatus(JobStatus newStatus) {
            Preconditions.checkState(
                    testingRunner != null,
                    "JobManagerRunner must be created before this method is available");
            this.testingRunner.setJobStatus(newStatus);
        }
    }

    private static final class QueueJobManagerRunnerFactory implements JobManagerRunnerFactory {
        private final Queue<? extends JobManagerRunner> jobManagerRunners;

        private QueueJobManagerRunnerFactory(Queue<? extends JobManagerRunner> jobManagerRunners) {
            this.jobManagerRunners = jobManagerRunners;
        }

        @Override
        public JobManagerRunner createJobManagerRunner(
                JobGraph jobGraph,
                Configuration configuration,
                RpcService rpcService,
                HighAvailabilityServices highAvailabilityServices,
                HeartbeatServices heartbeatServices,
                JobManagerSharedServices jobManagerServices,
                JobManagerJobMetricGroupFactory jobManagerJobMetricGroupFactory,
                FatalErrorHandler fatalErrorHandler,
                long initializationTimestamp) {
            return Optional.ofNullable(jobManagerRunners.poll())
                    .orElseThrow(
                            () ->
                                    new IllegalStateException(
                                            "Cannot create more JobManagerRunners."));
        }
    }

    private class FailingJobManagerRunnerFactory implements JobManagerRunnerFactory {
        private final Exception testException;

        public FailingJobManagerRunnerFactory(FlinkException testException) {
            this.testException = testException;
        }

        @Override
        public JobManagerRunner createJobManagerRunner(
                JobGraph jobGraph,
                Configuration configuration,
                RpcService rpcService,
                HighAvailabilityServices highAvailabilityServices,
                HeartbeatServices heartbeatServices,
                JobManagerSharedServices jobManagerServices,
                JobManagerJobMetricGroupFactory jobManagerJobMetricGroupFactory,
                FatalErrorHandler fatalErrorHandler,
                long initializationTimestamp)
                throws Exception {
            throw testException;
        }
    }
}
