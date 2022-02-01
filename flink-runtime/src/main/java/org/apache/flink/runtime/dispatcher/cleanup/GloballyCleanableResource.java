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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

/**
 * {@code GloballyCleanableResource} is supposed to be used by any class that provides artifacts for
 * a given job that can be cleaned up globally. Globally available artifacts should survive a
 * JobManager failover and are, in contrast to {@link GloballyCleanableResource}, only cleaned up
 * after the corresponding job reached a globally-terminal state.
 *
 * @see org.apache.flink.api.common.JobStatus
 */
@FunctionalInterface
public interface GloballyCleanableResource {

    void globalCleanup(JobID jobId) throws Throwable;

    default CompletableFuture<Void> globalCleanupAsync(JobID jobId, Executor cleanupExecutor) {
        return CompletableFuture.runAsync(
                () -> {
                    try {
                        globalCleanup(jobId);
                    } catch (Throwable t) {
                        throw new CompletionException("Asynchronous global cleanup failed", t);
                    }
                },
                cleanupExecutor);
    }
}
