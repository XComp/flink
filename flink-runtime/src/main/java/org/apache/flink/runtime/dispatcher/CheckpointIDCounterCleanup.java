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
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.util.Preconditions;

/**
 * {@code JobCheckpointIDCounterCleanup} cleans up the checkpoint ID counter metadata of given jobs.
 *
 * @see org.apache.flink.runtime.checkpoint.CheckpointIDCounter
 */
// TODO: no tests provided, yet
public class CheckpointIDCounterCleanup {

    private final CheckpointRecoveryFactory checkpointRecoveryFactory;

    public CheckpointIDCounterCleanup(CheckpointRecoveryFactory checkpointRecoveryFactory) {
        this.checkpointRecoveryFactory =
                Preconditions.checkNotNull(checkpointRecoveryFactory, "CheckpointRecoveryFactory");
    }

    public void cleanupJobData(JobID jobId, JobStatus jobStatus) throws Exception {
        checkpointRecoveryFactory.createCheckpointIDCounter(jobId).shutdown(jobStatus);
    }
}
