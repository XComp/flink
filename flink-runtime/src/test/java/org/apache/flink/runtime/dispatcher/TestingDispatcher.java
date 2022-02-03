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
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.dispatcher.cleanup.DispatcherResourceCleanerFactory;
import org.apache.flink.runtime.dispatcher.cleanup.ResourceCleanerFactory;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.JobResultStore;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.highavailability.nonha.embedded.EmbeddedJobResultStore;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.JobGraphWriter;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.metrics.groups.JobManagerMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Function;

/** {@link Dispatcher} implementation used for testing purposes. */
class TestingDispatcher extends Dispatcher {

    private final CompletableFuture<Void> startFuture;

    TestingDispatcher(
            RpcService rpcService,
            DispatcherId fencingToken,
            Collection<JobGraph> recoveredJobs,
            Collection<JobResult> recoveredDirtyJobResults,
            DispatcherBootstrapFactory dispatcherBootstrapFactory,
            DispatcherServices dispatcherServices)
            throws Exception {
        super(
                rpcService,
                fencingToken,
                recoveredJobs,
                recoveredDirtyJobResults,
                dispatcherBootstrapFactory,
                dispatcherServices);

        this.startFuture = new CompletableFuture<>();
    }

    private TestingDispatcher(
            RpcService rpcService,
            DispatcherId fencingToken,
            Collection<JobGraph> recoveredJobs,
            Collection<JobResult> recoveredDirtyJobs,
            Configuration configuration,
            HighAvailabilityServices highAvailabilityServices,
            GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever,
            HeartbeatServices heartbeatServices,
            BlobServer blobServer,
            FatalErrorHandler fatalErrorHandler,
            JobGraphWriter jobGraphWriter,
            JobResultStore jobResultStore,
            JobManagerMetricGroup jobManagerMetricGroup,
            @Nullable String metricServiceQueryAddress,
            Executor ioExecutor,
            HistoryServerArchivist historyServerArchivist,
            ExecutionGraphInfoStore executionGraphInfoStore,
            JobManagerRunnerFactory jobManagerRunnerFactory,
            DispatcherBootstrapFactory dispatcherBootstrapFactory,
            DispatcherOperationCaches dispatcherOperationCaches,
            JobManagerRunnerRegistry jobManagerRunnerRegistry,
            ResourceCleanerFactory resourceCleanerFactory)
            throws Exception {
        super(
                rpcService,
                fencingToken,
                recoveredJobs,
                recoveredDirtyJobs,
                configuration,
                highAvailabilityServices,
                resourceManagerGatewayRetriever,
                heartbeatServices,
                blobServer,
                fatalErrorHandler,
                jobGraphWriter,
                jobResultStore,
                jobManagerMetricGroup,
                metricServiceQueryAddress,
                ioExecutor,
                historyServerArchivist,
                executionGraphInfoStore,
                jobManagerRunnerFactory,
                dispatcherBootstrapFactory,
                dispatcherOperationCaches,
                jobManagerRunnerRegistry,
                resourceCleanerFactory);

        this.startFuture = new CompletableFuture<>();
    }

    @Override
    public void onStart() throws Exception {
        try {
            super.onStart();
        } catch (Exception e) {
            startFuture.completeExceptionally(e);
            throw e;
        }

        startFuture.complete(null);
    }

    void completeJobExecution(ExecutionGraphInfo executionGraphInfo) {
        runAsync(
                () -> {
                    try {
                        jobReachedTerminalState(executionGraphInfo);
                    } catch (Exception e) {
                        throw new CompletionException(e);
                    }
                });
    }

    CompletableFuture<Void> getJobTerminationFuture(@Nonnull JobID jobId, @Nonnull Time timeout) {
        return callAsyncWithoutFencing(() -> getJobTerminationFuture(jobId), timeout)
                .thenCompose(Function.identity());
    }

    CompletableFuture<Integer> getNumberJobs(Time timeout) {
        return callAsyncWithoutFencing(() -> listJobs(timeout).get().size(), timeout);
    }

    void waitUntilStarted() {
        startFuture.join();
    }

    public static TestingDispatcher.Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private RpcService rpcService = new TestingRpcService();
        private DispatcherId fencingToken = DispatcherId.generate();
        private Collection<JobGraph> recoveredJobs = Collections.emptyList();
        private Collection<JobResult> recoveredDirtyJobs = Collections.emptyList();
        private HighAvailabilityServices highAvailabilityServices =
                new TestingHighAvailabilityServices();

        private TestingResourceManagerGateway resourceManagerGateway =
                new TestingResourceManagerGateway();
        private GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever =
                () -> CompletableFuture.completedFuture(resourceManagerGateway);
        private HeartbeatServices heartbeatServices = new HeartbeatServices(1000L, 1000L);

        private JobGraphWriter jobGraphWriter = NoOpJobGraphWriter.INSTANCE;
        private JobResultStore jobResultStore = new EmbeddedJobResultStore();

        private Configuration configuration = new Configuration();

        // even-though it's labeled as @Nullable, it's a mandatory field that needs to be set before
        // building the Dispatcher instance
        @Nullable private BlobServer blobServer = null;
        private FatalErrorHandler fatalErrorHandler = new TestingFatalErrorHandler();
        private JobManagerMetricGroup jobManagerMetricGroup =
                UnregisteredMetricGroups.createUnregisteredJobManagerMetricGroup();
        @Nullable private String metricServiceQueryAddress = null;
        private Executor ioExecutor = ForkJoinPool.commonPool();
        private HistoryServerArchivist historyServerArchivist = VoidHistoryServerArchivist.INSTANCE;
        private ExecutionGraphInfoStore executionGraphInfoStore =
                new MemoryExecutionGraphInfoStore();
        private JobManagerRunnerFactory jobManagerRunnerFactory =
                new TestingJobManagerRunnerFactory(0);
        private DispatcherBootstrapFactory dispatcherBootstrapFactory =
                (dispatcher, scheduledExecutor, errorHandler) -> new NoOpDispatcherBootstrap();
        private DispatcherOperationCaches dispatcherOperationCaches =
                new DispatcherOperationCaches();
        private JobManagerRunnerRegistry jobManagerRunnerRegistry =
                new DefaultJobManagerRunnerRegistry(1);
        @Nullable private ResourceCleanerFactory resourceCleanerFactory;

        public Builder withRpcService(RpcService rpcService) {
            this.rpcService = rpcService;
            return this;
        }

        public Builder withFencingToken(DispatcherId fencingToken) {
            this.fencingToken = fencingToken;
            return this;
        }

        public Builder withRecoveredJobs(Collection<JobGraph> recoveredJobs) {
            this.recoveredJobs = recoveredJobs;
            return this;
        }

        public Builder withRecoveredDirtyJobs(Collection<JobResult> recoveredDirtyJobs) {
            this.recoveredDirtyJobs = recoveredDirtyJobs;
            return this;
        }

        public Builder withHighAvailabilityServices(
                HighAvailabilityServices highAvailabilityServices) {
            this.highAvailabilityServices = highAvailabilityServices;
            return this;
        }

        public Builder withResourceManagerGateway(
                TestingResourceManagerGateway resourceManagerGateway) {
            this.resourceManagerGateway = resourceManagerGateway;
            return this;
        }

        public Builder withResourceManagerGatewayRetriever(
                GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever) {
            this.resourceManagerGatewayRetriever = resourceManagerGatewayRetriever;
            return this;
        }

        public Builder withHeartbeatServices(HeartbeatServices heartbeatServices) {
            this.heartbeatServices = heartbeatServices;
            return this;
        }

        public Builder withJobGraphWriter(JobGraphWriter jobGraphWriter) {
            this.jobGraphWriter = jobGraphWriter;
            return this;
        }

        public Builder withJobResultStore(JobResultStore jobResultStore) {
            this.jobResultStore = jobResultStore;
            return this;
        }

        public Builder withConfiguration(Configuration configuration) {
            this.configuration = configuration;
            return this;
        }

        public Builder withBlobServer(BlobServer blobServer) {
            this.blobServer = blobServer;
            return this;
        }

        public Builder withFatalErrorHandler(FatalErrorHandler fatalErrorHandler) {
            this.fatalErrorHandler = fatalErrorHandler;
            return this;
        }

        public Builder withJobManagerMetricGroup(JobManagerMetricGroup jobManagerMetricGroup) {
            this.jobManagerMetricGroup = jobManagerMetricGroup;
            return this;
        }

        public Builder withMetricServiceQueryAddress(@Nullable String metricServiceQueryAddress) {
            this.metricServiceQueryAddress = metricServiceQueryAddress;
            return this;
        }

        public Builder withIoExecutor(Executor ioExecutor) {
            this.ioExecutor = ioExecutor;
            return this;
        }

        public Builder withHistoryServerArchivist(HistoryServerArchivist historyServerArchivist) {
            this.historyServerArchivist = historyServerArchivist;
            return this;
        }

        public Builder withExecutionGraphInfoStore(
                ExecutionGraphInfoStore executionGraphInfoStore) {
            this.executionGraphInfoStore = executionGraphInfoStore;
            return this;
        }

        public Builder withJobManagerRunnerFactory(
                JobManagerRunnerFactory jobManagerRunnerFactory) {
            this.jobManagerRunnerFactory = jobManagerRunnerFactory;
            return this;
        }

        public Builder withDispatcherBootstrapFactory(
                DispatcherBootstrapFactory dispatcherBootstrapFactory) {
            this.dispatcherBootstrapFactory = dispatcherBootstrapFactory;
            return this;
        }

        public Builder withDispatcherOperationCaches(
                DispatcherOperationCaches dispatcherOperationCaches) {
            this.dispatcherOperationCaches = dispatcherOperationCaches;
            return this;
        }

        public Builder withJobManagerRunnerRegistry(
                JobManagerRunnerRegistry jobManagerRunnerRegistry) {
            this.jobManagerRunnerRegistry = jobManagerRunnerRegistry;
            return this;
        }

        public Builder withResourceCleanerFactory(ResourceCleanerFactory resourceCleanerFactory) {
            this.resourceCleanerFactory = resourceCleanerFactory;
            return this;
        }

        private ResourceCleanerFactory createDefaultResourceCleanerFactory() {
            return new DispatcherResourceCleanerFactory(
                    ioExecutor,
                    jobManagerRunnerRegistry,
                    jobGraphWriter,
                    blobServer,
                    highAvailabilityServices,
                    jobManagerMetricGroup);
        }

        public TestingDispatcher build() throws Exception {
            return new TestingDispatcher(
                    rpcService,
                    fencingToken,
                    recoveredJobs,
                    recoveredDirtyJobs,
                    configuration,
                    highAvailabilityServices,
                    resourceManagerGatewayRetriever,
                    heartbeatServices,
                    Preconditions.checkNotNull(
                            blobServer,
                            "No BlobServer is specified for building the TestingDispatcher"),
                    fatalErrorHandler,
                    jobGraphWriter,
                    jobResultStore,
                    jobManagerMetricGroup,
                    metricServiceQueryAddress,
                    ioExecutor,
                    historyServerArchivist,
                    executionGraphInfoStore,
                    jobManagerRunnerFactory,
                    dispatcherBootstrapFactory,
                    dispatcherOperationCaches,
                    jobManagerRunnerRegistry,
                    resourceCleanerFactory != null
                            ? resourceCleanerFactory
                            : createDefaultResourceCleanerFactory());
        }
    }
}
