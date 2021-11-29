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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.CheckpointsCleaner;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.SharedStateRegistryFactory;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;

/**
 * {@code JobCheckpointStoreCleanup} cleans up left-over checkpoint data of given jobs.
 *
 * @see CompletedCheckpointStore
 */
// TODO: no tests provided, yet
public class CompletedCheckpointStoreCleanup {

    private static final Logger logger =
            LoggerFactory.getLogger(CompletedCheckpointStoreCleanup.class);

    private final CheckpointsCleaner checkpointsCleaner;
    private final Configuration jobManagerConfiguration;

    private final CheckpointRecoveryFactory checkpointRecoveryFactory;
    private final SharedStateRegistryFactory sharedStateRegistryFactory;

    private final Executor executor;

    public CompletedCheckpointStoreCleanup(
            CheckpointsCleaner checkpointsCleaner,
            Configuration jobManagerConfiguration,
            CheckpointRecoveryFactory checkpointRecoveryFactory,
            SharedStateRegistryFactory sharedStateRegistryFactory,
            Executor executor) {
        this.checkpointsCleaner =
                Preconditions.checkNotNull(checkpointsCleaner, "CheckpointsCleaner");
        this.jobManagerConfiguration =
                Preconditions.checkNotNull(jobManagerConfiguration, "JobManagerConfiguration");
        this.checkpointRecoveryFactory =
                Preconditions.checkNotNull(checkpointRecoveryFactory, "CheckpointRecoveryFactory");
        this.sharedStateRegistryFactory =
                Preconditions.checkNotNull(
                        sharedStateRegistryFactory, "SharedStateRegistryFactory");
        this.executor = Preconditions.checkNotNull(executor, "Executor");
    }

    public void cleanupJobData(JobID jobId, JobStatus jobStatus) throws Exception {
        final CompletedCheckpointStore completedCheckpointStore =
                checkpointRecoveryFactory.createRecoveredCompletedCheckpointStore(
                        jobId, jobManagerConfiguration, logger);

        createSharedStateFromCheckpoints(completedCheckpointStore, jobStatus);
    }

    private void createSharedStateFromCheckpoints(
            CompletedCheckpointStore completedCheckpointStore, JobStatus jobStatus)
            throws Exception {
        try (SharedStateRegistry sharedStateRegistry =
                sharedStateRegistryFactory.create(executor)) {
            this.collectSharedStateFromCheckpoints(completedCheckpointStore, sharedStateRegistry);

            completedCheckpointStore.shutdown(jobStatus, checkpointsCleaner);
        }
    }

    private void collectSharedStateFromCheckpoints(
            CompletedCheckpointStore completedCheckpointStore,
            SharedStateRegistry sharedStateRegistry)
            throws Exception {
        // register all (shared) states from the checkpoint store with the new registry
        for (CompletedCheckpoint completedCheckpoint :
                completedCheckpointStore.getAllCheckpoints()) {
            completedCheckpoint.registerSharedStatesAfterRestored(sharedStateRegistry);
        }
    }
}
