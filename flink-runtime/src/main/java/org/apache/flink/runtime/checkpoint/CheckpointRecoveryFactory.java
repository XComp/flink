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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;

import org.slf4j.Logger;

/** A factory for per Job checkpoint recovery components. */
public interface CheckpointRecoveryFactory {

    /**
     * Creates a RECOVERED {@link CompletedCheckpointStore} instance for a job. In this context,
     * RECOVERED means, that if we already have completed checkpoints from previous runs, we should
     * use them as the initial state.
     *
     * @param jobId Job ID to recover checkpoints for
     * @param maxNumberOfCheckpointsToRetain Maximum number of checkpoints to retain
     * @return {@link CompletedCheckpointStore} instance for the job
     */
    CompletedCheckpointStore createRecoveredCompletedCheckpointStore(
            JobID jobId, int maxNumberOfCheckpointsToRetain) throws Exception;

    /**
     * Instantiates the {@link CompletedCheckpointStore} based on the passed {@code Configuration}.
     *
     * @param jobId The {@code JobID} for which the {@code CompletedCheckpointStore} shall be
     *     created.
     * @param config The {@code Configuration} that shall be used (see {@link
     *     CheckpointingOptions#MAX_RETAINED_CHECKPOINTS}.
     * @param logger The logger that shall be used internally.
     * @return The {@code CompletedCheckpointStore} instance for the given {@code Job}.
     * @throws Exception if an error occurs while instantiating the {@code
     *     CompletedCheckpointStore}.
     */
    default CompletedCheckpointStore createRecoveredCompletedCheckpointStore(
            JobID jobId, Configuration config, Logger logger) throws Exception {
        int maxNumberOfCheckpointsToRetain =
                config.getInteger(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS);

        if (maxNumberOfCheckpointsToRetain <= 0) {
            // warning and use 1 as the default value if the setting in
            // state.checkpoints.max-retained-checkpoints is not greater than 0.
            logger.warn(
                    "The setting for '{} : {}' is invalid. Using default value of {}",
                    CheckpointingOptions.MAX_RETAINED_CHECKPOINTS.key(),
                    maxNumberOfCheckpointsToRetain,
                    CheckpointingOptions.MAX_RETAINED_CHECKPOINTS.defaultValue());

            maxNumberOfCheckpointsToRetain =
                    CheckpointingOptions.MAX_RETAINED_CHECKPOINTS.defaultValue();
        }

        return this.createRecoveredCompletedCheckpointStore(jobId, maxNumberOfCheckpointsToRetain);
    }

    /**
     * Creates a {@link CheckpointIDCounter} instance for a job.
     *
     * @param jobId Job ID to recover checkpoints for
     * @return {@link CheckpointIDCounter} instance for the job
     */
    CheckpointIDCounter createCheckpointIDCounter(JobID jobId) throws Exception;
}
