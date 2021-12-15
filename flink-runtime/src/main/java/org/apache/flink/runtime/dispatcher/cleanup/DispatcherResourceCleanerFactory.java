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

import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.dispatcher.DispatcherServices;
import org.apache.flink.runtime.dispatcher.JobManagerRunnerRegistry;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.jobmanager.JobGraphWriter;
import org.apache.flink.runtime.metrics.groups.JobManagerMetricGroup;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;
import java.util.concurrent.Executor;

/**
 * {@code DispatcherResourceCleanerFactory} instantiates {@link ResourceCleaner} instances that
 * clean cleanable resources from the {@link org.apache.flink.runtime.dispatcher.Dispatcher}.
 */
public class DispatcherResourceCleanerFactory implements ResourceCleanerFactory {

    private final Executor cleanupExecutor;
    private final JobManagerRunnerRegistry jobManagerRunnerRegistry;
    private final JobGraphWriter jobGraphWriter;
    private final BlobServer blobServer;
    private final HighAvailabilityServices highAvailabilityServices;
    private final JobManagerMetricGroup jobManagerMetricGroup;

    public DispatcherResourceCleanerFactory(
            JobManagerRunnerRegistry jobManagerRunnerRegistry,
            DispatcherServices dispatcherServices) {
        this(
                dispatcherServices.getIoExecutor(),
                jobManagerRunnerRegistry,
                dispatcherServices.getJobGraphWriter(),
                dispatcherServices.getBlobServer(),
                dispatcherServices.getHighAvailabilityServices(),
                dispatcherServices.getJobManagerMetricGroup());
    }

    private DispatcherResourceCleanerFactory(
            Executor cleanupExecutor,
            JobManagerRunnerRegistry jobManagerRunnerRegistry,
            JobGraphWriter jobGraphWriter,
            BlobServer blobServer,
            HighAvailabilityServices highAvailabilityServices,
            JobManagerMetricGroup jobManagerMetricGroup) {
        this.cleanupExecutor = Preconditions.checkNotNull(cleanupExecutor);
        this.jobManagerRunnerRegistry = Preconditions.checkNotNull(jobManagerRunnerRegistry);
        this.jobGraphWriter = Preconditions.checkNotNull(jobGraphWriter);
        this.blobServer = Preconditions.checkNotNull(blobServer);
        this.highAvailabilityServices = Preconditions.checkNotNull(highAvailabilityServices);
        this.jobManagerMetricGroup = Preconditions.checkNotNull(jobManagerMetricGroup);
    }

    public ResourceCleaner createLocalResourceCleaner() {
        final DefaultResourceCleaner resourceCleaner = new DefaultResourceCleaner(cleanupExecutor);
        for (LocallyCleanableResource locallyCleanableResource :
                Arrays.asList(
                        jobManagerRunnerRegistry,
                        jobGraphWriter,
                        blobServer,
                        highAvailabilityServices,
                        jobManagerMetricGroup)) {
            resourceCleaner.withCleanupOf(locallyCleanableResource::localCleanupAsync);
        }

        return resourceCleaner;
    }

    public ResourceCleaner createGlobalResourceCleaner() {
        final DefaultResourceCleaner resourceCleaner = new DefaultResourceCleaner(cleanupExecutor);
        for (GloballyCleanableResource locallyCleanableResource :
                Arrays.asList(
                        jobManagerRunnerRegistry,
                        jobGraphWriter,
                        blobServer,
                        highAvailabilityServices,
                        jobManagerMetricGroup)) {
            resourceCleaner.withCleanupOf(locallyCleanableResource::globalCleanupAsync);
        }

        return resourceCleaner;
    }
}
