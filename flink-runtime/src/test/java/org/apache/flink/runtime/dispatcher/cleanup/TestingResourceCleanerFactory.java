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

import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.concurrent.Executors;
import org.apache.flink.util.concurrent.FutureUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Executor;

/** {@code TestingResourceCleanerFactory} for adding custom {@link ResourceCleaner} creation. */
public class TestingResourceCleanerFactory implements ResourceCleanerFactory {

    private final Collection<LocallyCleanableResource> locallyCleanableResources =
            new ArrayList<>();
    private final Collection<GloballyCleanableResource> globallyCleanableResources =
            new ArrayList<>();

    private final Executor cleanupExecutor;

    public TestingResourceCleanerFactory() {
        this(Executors.directExecutor());
    }

    private TestingResourceCleanerFactory(Executor cleanupExecutor) {
        this.cleanupExecutor = cleanupExecutor;
    }

    public TestingResourceCleanerFactory withLocallyCleanableResource(
            LocallyCleanableResource locallyCleanableResource) {
        this.locallyCleanableResources.add(locallyCleanableResource);

        return this;
    }

    public TestingResourceCleanerFactory withGloballyCleanableResource(
            GloballyCleanableResource globallyCleanableResource) {
        this.globallyCleanableResources.add(globallyCleanableResource);

        return this;
    }

    @Override
    public ResourceCleaner createLocalResourceCleaner(
            ComponentMainThreadExecutor mainThreadExecutor) {
        return jobId -> {
            mainThreadExecutor.assertRunningInMainThread();
            Throwable t = null;
            for (LocallyCleanableResource locallyCleanableResource : locallyCleanableResources) {
                try {
                    locallyCleanableResource.localCleanupAsync(jobId, cleanupExecutor).get();
                } catch (Throwable throwable) {
                    t = ExceptionUtils.firstOrSuppressed(throwable, t);
                }
            }

            return t != null
                    ? FutureUtils.completedExceptionally(t)
                    : FutureUtils.completedVoidFuture();
        };
    }

    @Override
    public ResourceCleaner createGlobalResourceCleaner(
            ComponentMainThreadExecutor mainThreadExecutor) {
        return jobId -> {
            mainThreadExecutor.assertRunningInMainThread();
            Throwable t = null;
            for (GloballyCleanableResource globallyCleanableResource : globallyCleanableResources) {
                try {
                    globallyCleanableResource.globalCleanupAsync(jobId, cleanupExecutor).get();
                } catch (Throwable throwable) {
                    t = ExceptionUtils.firstOrSuppressed(throwable, t);
                }
            }

            return t != null
                    ? FutureUtils.completedExceptionally(t)
                    : FutureUtils.completedVoidFuture();
        };
    }
}
