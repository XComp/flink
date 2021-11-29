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

package org.apache.flink.runtime.testutils;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.highavailability.JobResultStore;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.util.function.FunctionWithException;
import org.apache.flink.util.function.SupplierWithException;
import org.apache.flink.util.function.ThrowingConsumer;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

/**
 * {@code TestingJobResultStore} is a {@link JobResultStore} implementation that can be used in
 * tests.
 */
public class TestingJobResultStore implements JobResultStore {

    private final ThrowingConsumer<JobResult, ? extends IOException> createDirtyResultConsumer;
    private final ThrowingConsumer<JobID, ? extends IOException> markResultAsCleanConsumer;
    private final FunctionWithException<JobID, Boolean, ? extends IOException>
            hasJobResultEntryFunction;
    private final SupplierWithException<Collection<JobResult>, ? extends IOException>
            getDirtyResultsSupplier;

    private TestingJobResultStore(
            ThrowingConsumer<JobResult, ? extends IOException> createDirtyResultConsumer,
            ThrowingConsumer<JobID, ? extends IOException> markResultAsCleanConsumer,
            FunctionWithException<JobID, Boolean, ? extends IOException> hasJobResultEntryFunction,
            SupplierWithException<Collection<JobResult>, ? extends IOException>
                    getDirtyResultsSupplier) {
        this.createDirtyResultConsumer = createDirtyResultConsumer;
        this.markResultAsCleanConsumer = markResultAsCleanConsumer;
        this.hasJobResultEntryFunction = hasJobResultEntryFunction;
        this.getDirtyResultsSupplier = getDirtyResultsSupplier;
    }

    @Override
    public void createDirtyResult(JobResult jobResult) throws IOException {
        createDirtyResultConsumer.accept(jobResult);
    }

    @Override
    public void markResultAsClean(JobID jobId) throws IOException {
        markResultAsCleanConsumer.accept(jobId);
    }

    @Override
    public boolean hasJobResultEntry(JobID jobId) throws IOException {
        return hasJobResultEntryFunction.apply(jobId);
    }

    @Override
    public Collection<JobResult> getDirtyResults() throws IOException {
        return getDirtyResultsSupplier.get();
    }

    public static TestingJobResultStore.Builder builder() {
        return new Builder();
    }

    /** {@code Builder} for instantiating {@code TestingJobResultStore} instances. */
    public static class Builder {

        private ThrowingConsumer<JobResult, ? extends IOException> createDirtyResultConsumer =
                ignored -> {};
        private ThrowingConsumer<JobID, ? extends IOException> markResultAsCleanConsumer =
                ignored -> {};
        private FunctionWithException<JobID, Boolean, ? extends IOException>
                hasJobResultEntryFunction = ignored -> false;
        private SupplierWithException<Collection<JobResult>, ? extends IOException>
                getDirtyResultsSupplier = Collections::emptyList;

        public Builder withCreateDirtyResultConsumer(
                ThrowingConsumer<JobResult, ? extends IOException> createDirtyResultConsumer) {
            this.createDirtyResultConsumer = createDirtyResultConsumer;
            return this;
        }

        public Builder withMarkResultAsCleanConsumer(
                ThrowingConsumer<JobID, ? extends IOException> markResultAsCleanConsumer) {
            this.markResultAsCleanConsumer = markResultAsCleanConsumer;
            return this;
        }

        public Builder withHasJobResultEntryFunction(
                FunctionWithException<JobID, Boolean, ? extends IOException>
                        hasJobResultEntryFunction) {
            this.hasJobResultEntryFunction = hasJobResultEntryFunction;
            return this;
        }

        public Builder withGetDirtyResultsSupplier(
                SupplierWithException<Collection<JobResult>, ? extends IOException>
                        getDirtyResultsSupplier) {
            this.getDirtyResultsSupplier = getDirtyResultsSupplier;
            return this;
        }

        public TestingJobResultStore build() {
            return new TestingJobResultStore(
                    createDirtyResultConsumer,
                    markResultAsCleanConsumer,
                    hasJobResultEntryFunction,
                    getDirtyResultsSupplier);
        }
    }
}
