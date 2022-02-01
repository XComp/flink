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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.BlobStore;
import org.apache.flink.runtime.blob.TestingBlobStoreBuilder;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.dispatcher.DefaultJobManagerRunnerRegistry;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.JobResultStore;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.JobGraphStore;
import org.apache.flink.runtime.jobmanager.JobGraphWriter;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.groups.JobManagerMetricGroup;
import org.apache.flink.runtime.metrics.util.TestingMetricRegistry;
import org.apache.flink.util.concurrent.Executors;
import org.apache.flink.util.concurrent.FutureUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code DispatcherResourceCleanerFactoryTest} verifies that the resources are properly cleaned up
 * for both, the {@link GloballyCleanableResource} and {@link LocallyCleanableResource} of the
 * {@link org.apache.flink.runtime.dispatcher.Dispatcher}.
 */
public class DispatcherResourceCleanerFactoryTest {

    private static final JobID JOB_ID = new JobID();

    private CleanableJobManagerRegistry jobManagerRunnerRegistry;
    private CleanableJobGraphWriter jobGraphWriter;
    private CleanableBlobServer blobServer;
    private CleanableHighAvailabilityServices highAvailabilityServices;
    private JobManagerMetricGroup jobManagerMetricGroup;

    private DispatcherResourceCleanerFactory testInstance;

    @BeforeEach
    public void setup() throws IOException {
        jobManagerRunnerRegistry = new CleanableJobManagerRegistry();
        jobGraphWriter = new CleanableJobGraphWriter();
        blobServer = new CleanableBlobServer();
        highAvailabilityServices = new CleanableHighAvailabilityServices();

        MetricRegistry metricRegistry = TestingMetricRegistry.builder().build();
        jobManagerMetricGroup =
                JobManagerMetricGroup.createJobManagerMetricGroup(
                        metricRegistry, "ignored hostname");
        jobManagerMetricGroup.addJob(JOB_ID, "ignored job name");

        testInstance =
                new DispatcherResourceCleanerFactory(
                        Executors.directExecutor(),
                        jobManagerRunnerRegistry,
                        jobGraphWriter,
                        blobServer,
                        highAvailabilityServices,
                        jobManagerMetricGroup);
    }

    @Test
    public void testLocalResourceCleaning() {
        assertGlobalCleanupNotTriggered();
        assertLocalCleanupNotTriggered();

        final CompletableFuture<Void> cleanupResultFuture =
                testInstance.createLocalResourceCleaner().cleanupAsync(JOB_ID);

        assertGlobalCleanupNotTriggered();
        assertLocalCleanupTriggeredWaitingForJobManagerRunnerRegistry();

        assertThat(cleanupResultFuture).isNotCompleted();

        jobManagerRunnerRegistry.completeLocalCleanup();

        assertGlobalCleanupNotTriggered();
        assertLocalCleanupTriggered();

        assertThat(cleanupResultFuture).isCompleted();
    }

    @Test
    public void testGlobalResourceCleaning()
            throws ExecutionException, InterruptedException, TimeoutException {
        assertGlobalCleanupNotTriggered();
        assertLocalCleanupNotTriggered();

        final CompletableFuture<Void> cleanupResultFuture =
                testInstance.createGlobalResourceCleaner().cleanupAsync(JOB_ID);

        assertGlobalCleanupTriggered();
        assertLocalCleanupNotTriggered();

        assertThat(cleanupResultFuture).isCompleted();
    }

    private void assertLocalCleanupNotTriggered() {
        assertThat(jobManagerRunnerRegistry.getLocalCleanupFuture()).isNotDone();
        assertThat(jobGraphWriter.getLocalCleanupFuture()).isNotDone();
        assertThat(blobServer.getLocalCleanupFuture()).isNotDone();
        assertThat(highAvailabilityServices.getLocalCleanupFuture()).isNotDone();
        assertThat(jobManagerMetricGroup.numRegisteredJobMetricGroups()).isEqualTo(1);
    }

    private void assertLocalCleanupTriggeredWaitingForJobManagerRunnerRegistry() {
        assertThat(jobManagerRunnerRegistry.getLocalCleanupFuture()).isDone();

        // the JobManagerRunnerRegistry needs to be cleaned up first
        assertThat(jobGraphWriter.getLocalCleanupFuture()).isNotDone();
        assertThat(blobServer.getLocalCleanupFuture()).isNotDone();
        assertThat(highAvailabilityServices.getLocalCleanupFuture()).isNotDone();
        assertThat(jobManagerMetricGroup.numRegisteredJobMetricGroups()).isEqualTo(1);
    }

    private void assertGlobalCleanupNotTriggered() {
        assertThat(jobGraphWriter.getGlobalCleanupFuture()).isNotDone();
        assertThat(blobServer.getGlobalCleanupFuture()).isNotDone();
        assertThat(highAvailabilityServices.getGlobalCleanupFuture()).isNotDone();
    }

    private void assertLocalCleanupTriggered() {
        assertThat(jobManagerRunnerRegistry.getLocalCleanupFuture()).isCompleted();
        assertThat(jobGraphWriter.getLocalCleanupFuture()).isCompleted();
        assertThat(blobServer.getLocalCleanupFuture()).isCompleted();
        assertThat(highAvailabilityServices.getLocalCleanupFuture()).isNotCompleted();
        assertThat(jobManagerMetricGroup.numRegisteredJobMetricGroups()).isEqualTo(0);
    }

    private void assertGlobalCleanupTriggered() {
        assertThat(jobGraphWriter.getGlobalCleanupFuture()).isCompleted();
        assertThat(blobServer.getGlobalCleanupFuture()).isCompleted();
        assertThat(highAvailabilityServices.getGlobalCleanupFuture()).isCompleted();
    }

    private static class AbstractTestingCleanableResource
            implements LocallyCleanableResource, GloballyCleanableResource {

        private final CompletableFuture<JobID> localCleanupFuture = new CompletableFuture<>();
        private final CompletableFuture<JobID> globalCleanupFuture = new CompletableFuture<>();

        @Override
        public void globalCleanup(JobID jobId) {
            throw new UnsupportedOperationException("Synchronous globalCleanup is not supported.");
        }

        @Override
        public void localCleanup(JobID jobId) {
            throw new UnsupportedOperationException("Synchronous localCleanup is not supported.");
        }

        @Override
        public CompletableFuture<Void> localCleanupAsync(JobID jobId, Executor ignoredExecutor) {
            localCleanupFuture.complete(jobId);

            return FutureUtils.completedVoidFuture();
        }

        @Override
        public CompletableFuture<Void> globalCleanupAsync(JobID jobId, Executor ignoredExecutor) {
            globalCleanupFuture.complete(jobId);

            return FutureUtils.completedVoidFuture();
        }

        public CompletableFuture<JobID> getLocalCleanupFuture() {
            return localCleanupFuture;
        }

        public CompletableFuture<JobID> getGlobalCleanupFuture() {
            return globalCleanupFuture;
        }
    }

    private static class CleanableJobGraphWriter extends AbstractTestingCleanableResource
            implements JobGraphWriter {

        @Override
        public void putJobGraph(JobGraph jobGraph) {
            throw new UnsupportedOperationException("putJobGraph operation not supported.");
        }
    }

    private static class CleanableHighAvailabilityServices extends AbstractTestingCleanableResource
            implements HighAvailabilityServices {

        @Override
        public LeaderRetrievalService getResourceManagerLeaderRetriever() {
            throw new UnsupportedOperationException(
                    "getResourceManagerLeaderRetriever operation not supported.");
        }

        @Override
        public LeaderRetrievalService getDispatcherLeaderRetriever() {
            throw new UnsupportedOperationException(
                    "getDispatcherLeaderRetriever operation not supported.");
        }

        @Override
        public LeaderRetrievalService getJobManagerLeaderRetriever(JobID jobID) {
            throw new UnsupportedOperationException(
                    "getJobManagerLeaderRetriever operation not supported.");
        }

        @Override
        public LeaderRetrievalService getJobManagerLeaderRetriever(
                JobID jobID, String defaultJobManagerAddress) {
            throw new UnsupportedOperationException(
                    "getJobManagerLeaderRetriever operation not supported.");
        }

        @Override
        public LeaderElectionService getResourceManagerLeaderElectionService() {
            throw new UnsupportedOperationException(
                    "getResourceManagerLeaderElectionService operation not supported.");
        }

        @Override
        public LeaderElectionService getDispatcherLeaderElectionService() {
            throw new UnsupportedOperationException(
                    "getDispatcherLeaderElectionService operation not supported.");
        }

        @Override
        public LeaderElectionService getJobManagerLeaderElectionService(JobID jobID) {
            throw new UnsupportedOperationException(
                    "getJobManagerLeaderElectionService operation not supported.");
        }

        @Override
        public CheckpointRecoveryFactory getCheckpointRecoveryFactory() throws Exception {
            throw new UnsupportedOperationException(
                    "getCheckpointRecoveryFactory operation not supported.");
        }

        @Override
        public JobGraphStore getJobGraphStore() throws Exception {
            throw new UnsupportedOperationException("getJobGraphStore operation not supported.");
        }

        @Override
        public JobResultStore getJobResultStore() throws Exception {
            throw new UnsupportedOperationException("getJobResultStore operation not supported.");
        }

        @Override
        public BlobStore createBlobStore() throws IOException {
            throw new UnsupportedOperationException("createBlobStore operation not supported.");
        }

        @Override
        public void close() throws Exception {
            throw new UnsupportedOperationException("close operation not supported.");
        }

        @Override
        public void closeAndCleanupAllData() throws Exception {
            throw new UnsupportedOperationException(
                    "closeAndCleanupAllData operation not supported.");
        }
    }

    private static class CleanableBlobServer extends BlobServer {

        private final CompletableFuture<JobID> localCleanupFuture = new CompletableFuture<>();
        private final CompletableFuture<JobID> globalCleanupFuture = new CompletableFuture<>();

        public CleanableBlobServer() throws IOException {
            super(
                    new Configuration(),
                    new File("non-existent-file"),
                    new TestingBlobStoreBuilder().createTestingBlobStore());
        }

        @Override
        public void globalCleanup(JobID jobId) {
            throw new UnsupportedOperationException("Synchronous globalCleanup is not supported.");
        }

        @Override
        public void localCleanup(JobID jobId) {
            throw new UnsupportedOperationException("Synchronous localCleanup is not supported.");
        }

        @Override
        public CompletableFuture<Void> localCleanupAsync(JobID jobId, Executor ignoredExecutor) {
            localCleanupFuture.complete(jobId);

            return FutureUtils.completedVoidFuture();
        }

        @Override
        public CompletableFuture<Void> globalCleanupAsync(JobID jobId, Executor ignoredExecutor) {
            globalCleanupFuture.complete(jobId);

            return FutureUtils.completedVoidFuture();
        }

        public CompletableFuture<JobID> getLocalCleanupFuture() {
            return localCleanupFuture;
        }

        public CompletableFuture<JobID> getGlobalCleanupFuture() {
            return globalCleanupFuture;
        }
    }

    private static class CleanableJobManagerRegistry extends DefaultJobManagerRunnerRegistry {

        private final CompletableFuture<JobID> localCleanupFuture = new CompletableFuture<>();

        private final CompletableFuture<Void> localCleanupResultFuture = new CompletableFuture<>();

        public CleanableJobManagerRegistry() {
            super(1);
        }

        @Override
        public void localCleanup(JobID jobId) {
            throw new UnsupportedOperationException("Synchronous localCleanup is not supported.");
        }

        @Override
        public CompletableFuture<Void> localCleanupAsync(JobID jobId, Executor ignoredExecutor) {
            localCleanupFuture.complete(jobId);

            return localCleanupResultFuture;
        }

        public CompletableFuture<JobID> getLocalCleanupFuture() {
            return localCleanupFuture;
        }

        public void completeLocalCleanup() {
            localCleanupResultFuture.complete(null);
        }
    }
}
