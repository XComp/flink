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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.dispatcher.cleanup.GloballyCleanableResource;
import org.apache.flink.runtime.dispatcher.cleanup.LocallyCleanableResource;
import org.apache.flink.runtime.jobmaster.JobManagerRunner;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/** {@code JobManagerRunner} collects running jobs represented by {@link JobManagerRunner}. */
public class JobManagerRunnerRegistry
        implements LocallyCleanableResource, GloballyCleanableResource {

    @VisibleForTesting final Map<JobID, JobManagerRunner> jobManagerRunners;

    public JobManagerRunnerRegistry(int initialCapacity) {
        Preconditions.checkArgument(initialCapacity > 0);
        jobManagerRunners = new HashMap<>(initialCapacity);
    }

    public boolean isRegistered(JobID jobId) {
        return jobManagerRunners.containsKey(jobId);
    }

    public void register(JobManagerRunner jobManagerRunner) {
        Preconditions.checkArgument(
                !isRegistered(jobManagerRunner.getJobID()),
                "A job with the ID %s is already registered.",
                jobManagerRunner.getJobID());
        this.jobManagerRunners.put(jobManagerRunner.getJobID(), jobManagerRunner);
    }

    /**
     * Returns the {@link JobManagerRunner} for the given {@code JobID}.
     *
     * @throws NoSuchElementException if the passed {@code JobID} does not belong to a registered
     *     {@code JobManagerRunner}.
     * @see #isRegistered(JobID)
     */
    public JobManagerRunner get(JobID jobId) {
        assertJobRegistered(jobId);
        return this.jobManagerRunners.get(jobId);
    }

    public int size() {
        return this.jobManagerRunners.size();
    }

    public Set<JobID> getRunningJobIds() {
        return this.jobManagerRunners.keySet();
    }

    public Collection<JobManagerRunner> getJobManagerRunners() {
        return this.jobManagerRunners.values();
    }

    @Override
    public void globalCleanup(JobID jobId) throws Exception {
        cleanup(jobId);
    }

    @Override
    public CompletableFuture<Void> globalCleanupAsync(JobID jobId, Executor unusedExecutor) {
        return cleanupAsync(jobId);
    }

    @Override
    public void localCleanup(JobID jobId) throws Exception {
        cleanup(jobId);
    }

    @Override
    public CompletableFuture<Void> localCleanupAsync(JobID jobId, Executor unusedExecutor) {
        return cleanupAsync(jobId);
    }

    private void cleanup(JobID jobId) throws Exception {
        if (isRegistered(jobId)) {
            unregister(jobId).close();
        }
    }

    private CompletableFuture<Void> cleanupAsync(JobID jobId) {
        if (!isRegistered(jobId)) {
            return FutureUtils.completedVoidFuture();
        }

        return unregister(jobId).closeAsync();
    }

    public JobManagerRunner unregister(JobID jobId) {
        assertJobRegistered(jobId);
        return this.jobManagerRunners.remove(jobId);
    }

    private void assertJobRegistered(JobID jobId) {
        if (!isRegistered(jobId)) {
            throw new NoSuchElementException(
                    "There is no running job registered for the job ID " + jobId);
        }
    }
}
