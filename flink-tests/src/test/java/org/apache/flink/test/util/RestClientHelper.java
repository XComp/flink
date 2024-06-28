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

package org.apache.flink.test.util;

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.rest.handler.legacy.messages.ClusterOverviewWithVersion;
import org.apache.flink.runtime.rest.messages.job.JobDetailsInfo;
import org.apache.flink.runtime.testutils.CommonTestUtils;

public class RestClientHelper {

    private final RestClusterClient<?> delegate;

    public RestClientHelper(RestClusterClient<?> delegate) {
        this.delegate = delegate;
    }

    public RestClusterClient<?> getRestClusterClient() {
        return delegate;
    }

    public int getNumberOfTasksByState(JobID jobId, ExecutionState state) {
        final JobDetailsInfo jobDetailsInfo = delegate.getJobDetails(jobId).join();
        return jobDetailsInfo.getJobVertexInfos().stream()
                .map(JobDetailsInfo.JobVertexDetailsInfo::getTasksPerState)
                .map(tasksPerState -> tasksPerState.get(state))
                .mapToInt(Integer::intValue)
                .sum();
    }

    public int getNumberOfRunningTasks(JobID jobId) {
        return getNumberOfTasksByState(jobId, ExecutionState.RUNNING);
    }

    public void waitForRunningTasks(JobID jobId, int expectedNumberOfRunningTasks)
            throws Exception {
        CommonTestUtils.waitUntilCondition(
                () -> {
                    final int numberOfRunningTasks = getNumberOfRunningTasks(jobId);
                    return numberOfRunningTasks == expectedNumberOfRunningTasks;
                });
    }

    public void waitForAvailableSlots(int expectedAvailableSlots) throws Exception {
        CommonTestUtils.waitUntilCondition(
                () -> {
                    final ClusterOverviewWithVersion clusterOverview =
                            delegate.getClusterOverview().join();
                    return clusterOverview.getNumSlotsAvailable() == expectedAvailableSlots;
                });
    }
}
