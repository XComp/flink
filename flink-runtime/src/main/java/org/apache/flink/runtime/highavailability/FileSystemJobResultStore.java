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

package org.apache.flink.runtime.highavailability;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.JobResultStoreOptions;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;

import static org.apache.flink.util.StringUtils.isNullOrWhitespaceOnly;

/**
 * An implementation of the {@link JobResultStore} which persists job result data to an underlying
 * distributed filesystem.
 */
public class FileSystemJobResultStore extends AbstractThreadsafeJobResultStore {

    private static final String DIRTY_SUFFIX = "_DIRTY";

    private final ObjectMapper mapper = new ObjectMapper();

    @GuardedBy("readWriteLock")
    private final FileSystem fileSystem;

    private final Path basePath;

    private final boolean deleteOnCommit;

    FileSystemJobResultStore(FileSystem fileSystem, Path basePath, boolean deleteOnCommit)
            throws IOException {
        this.fileSystem = fileSystem;
        this.basePath = basePath;
        this.deleteOnCommit = deleteOnCommit;

        this.fileSystem.mkdirs(this.basePath);
    }

    public static FileSystemJobResultStore fromConfiguration(Configuration config)
            throws IOException {
        Preconditions.checkNotNull(config);
        final String jrsStoragePath = config.get(JobResultStoreOptions.STORAGE_PATH);
        final Path basePath;

        if (isNullOrWhitespaceOnly(jrsStoragePath)) {
            final String haStoragePath = config.get(HighAvailabilityOptions.HA_STORAGE_PATH);
            final String haClusterId = config.get(HighAvailabilityOptions.HA_CLUSTER_ID);
            basePath = new Path(haStoragePath + "/job-result-store/" + haClusterId);
        } else {
            basePath = new Path(jrsStoragePath);
        }

        boolean deleteOnCommit = config.get(JobResultStoreOptions.DELETE_ON_COMMIT);

        return new FileSystemJobResultStore(basePath.getFileSystem(), basePath, deleteOnCommit);
    }

    /**
     * Given a job ID, construct the path for a dirty entry corresponding to it in the job result
     * store.
     *
     * @param jobId The job ID to construct a dirty entry path from.
     * @return A path for a dirty entry for the given the Job ID.
     */
    private Path constructDirtyPath(JobID jobId) {
        return new Path(this.basePath.getPath() + "/" + jobId.toString() + DIRTY_SUFFIX + ".json");
    }

    /**
     * Given a job ID, construct the path for a clean entry corresponding to it in the job result
     * store.
     *
     * @param jobId The job ID to construct a clean entry path from.
     * @return A path for a clean entry for the given the Job ID.
     */
    private Path constructCleanPath(JobID jobId) {
        return new Path(this.basePath.getPath() + "/" + jobId.toString() + ".json");
    }

    @Override
    public void createDirtyResultInternal(JobResultEntry jobResultEntry) throws IOException {
        final Path path = constructDirtyPath(jobResultEntry.getJobId());
        OutputStream os = fileSystem.create(path, FileSystem.WriteMode.NO_OVERWRITE);
        mapper.writeValue(os, new JobResultEntry.JsonJobResultEntry(jobResultEntry));
    }

    @Override
    public void markResultAsCleanInternal(JobID jobId) throws IOException, NoSuchElementException {
        Path dirtyPath = constructDirtyPath(jobId);
        if (fileSystem.exists(dirtyPath)) {
            if (deleteOnCommit) {
                fileSystem.delete(dirtyPath, false);
            } else {
                fileSystem.rename(dirtyPath, constructCleanPath(jobId));
            }
        } else {
            throw new NoSuchElementException(
                    String.format(
                            "Could not mark job %s as clean as it is not present in the job result store.",
                            jobId));
        }
    }

    @Override
    public boolean hasDirtyJobResultEntryInternal(JobID jobId) throws IOException {
        return fileSystem.exists(constructDirtyPath(jobId));
    }

    @Override
    public boolean hasCleanJobResultEntryInternal(JobID jobId) throws IOException {
        return fileSystem.exists(constructCleanPath(jobId));
    }

    @Override
    public Set<JobResult> getDirtyResultsInternal() throws IOException {
        final Set<JobResult> dirtyResults = new HashSet<>();
        final FileStatus fs = fileSystem.getFileStatus(this.basePath);
        if (fs.isDir()) {
            FileStatus[] statuses = fileSystem.listStatus(this.basePath);
            for (FileStatus s : statuses) {
                if (!s.isDir()) {
                    String path = s.getPath().getPath();
                    if (path.contains(DIRTY_SUFFIX)) {
                        JobResultEntry.JsonJobResultEntry jre =
                                mapper.readValue(
                                        fileSystem.open(s.getPath()),
                                        JobResultEntry.JsonJobResultEntry.class);
                        dirtyResults.add(jre.getJobResult());
                    }
                }
            }
        }
        return dirtyResults;
    }
}
