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
 * {@code LocallyCleanableResource} is supposed to be used by any class that provides artifacts for
 * a given job that can be cleaned up locally.
 *
 * @see GloballyCleanableResource
 */
@FunctionalInterface
public interface LocallyCleanableResource {

    void localCleanup(JobID jobId) throws Throwable;

    default CompletableFuture<Void> localCleanupAsync(JobID jobId, Executor cleanupExecutor) {
        return CompletableFuture.runAsync(
                () -> {
                    try {
                        localCleanup(jobId);
                    } catch (Throwable t) {
                        throw new CompletionException("Asynchronous local cleanup failed", t);
                    }
                },
                cleanupExecutor);
    }
}
