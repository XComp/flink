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
import org.apache.flink.util.function.ThrowingConsumer;

import java.io.IOException;

/**
 * {@code TestingCleanableResource} provides methods for testing the callbacks of {@link
 * LocallyCleanableResource}.
 */
class TestingCleanableResource implements LocallyCleanableResource, GloballyCleanableResource {

    private final ThrowingConsumer<JobID, IOException> localCleanupConsumer;
    private final ThrowingConsumer<JobID, IOException> globalCleanupConsumer;

    private TestingCleanableResource(
            ThrowingConsumer<JobID, IOException> localCleanupConsumer,
            ThrowingConsumer<JobID, IOException> globalCleanupConsumer) {
        this.localCleanupConsumer = localCleanupConsumer;
        this.globalCleanupConsumer = globalCleanupConsumer;
    }

    @Override
    public void localCleanup(JobID jobId) throws IOException {
        localCleanupConsumer.accept(jobId);
    }

    @Override
    public void globalCleanup(JobID jobId) throws IOException {
        globalCleanupConsumer.accept(jobId);
    }

    public static TestingCleanableResource.Builder builder() {
        return new Builder();
    }

    static class Builder {

        private ThrowingConsumer<JobID, IOException> localCleanupConsumer =
                jobId -> {
                    throw new UnsupportedOperationException("Local cleanup is not supported.");
                };
        private ThrowingConsumer<JobID, IOException> globalCleanupConsumer =
                jobId -> {
                    throw new UnsupportedOperationException("Global cleanup is not supported.");
                };

        private Builder() {}

        public Builder withLocalCleanupConsumer(
                ThrowingConsumer<JobID, IOException> localCleanupConsumer) {
            this.localCleanupConsumer = localCleanupConsumer;
            return this;
        }

        public Builder withGlobalCleanupConsumer(
                ThrowingConsumer<JobID, IOException> globalCleanupConsumer) {
            this.globalCleanupConsumer = globalCleanupConsumer;
            return this;
        }

        public TestingCleanableResource build() {
            return new TestingCleanableResource(localCleanupConsumer, globalCleanupConsumer);
        }
    }
}
