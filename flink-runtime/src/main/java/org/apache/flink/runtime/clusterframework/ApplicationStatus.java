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

package org.apache.flink.runtime.clusterframework;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava30.com.google.common.collect.BiMap;
import org.apache.flink.shaded.guava30.com.google.common.collect.HashBiMap;

/** The status of an application. */
public enum ApplicationStatus {

    /** Application finished successfully */
    SUCCEEDED(0),

    /** Application encountered an unrecoverable failure or error */
    FAILED(1443),

    /** Application was canceled or killed on request */
    CANCELED(0),

    /** Application status is not known */
    UNKNOWN(1445);

    // ------------------------------------------------------------------------

    private static final BiMap<JobStatus, ApplicationStatus> JOB_STATUS_APPLICATION_STATUS_BI_MAP =
            HashBiMap.create();

    static {
        // only globally-terminated JobStatus have a corresponding ApplicationStatus
        JOB_STATUS_APPLICATION_STATUS_BI_MAP.put(JobStatus.FAILED, ApplicationStatus.FAILED);
        JOB_STATUS_APPLICATION_STATUS_BI_MAP.put(JobStatus.CANCELED, ApplicationStatus.CANCELED);
        JOB_STATUS_APPLICATION_STATUS_BI_MAP.put(JobStatus.FINISHED, ApplicationStatus.SUCCEEDED);
    }

    /** The associated process exit code */
    private final int processExitCode;

    private ApplicationStatus(int exitCode) {
        this.processExitCode = exitCode;
    }

    /**
     * Gets the process exit code associated with this status
     *
     * @return The associated process exit code.
     */
    public int processExitCode() {
        return processExitCode;
    }

    /**
     * Derives the ApplicationStatus that should be used for a job that resulted in the given job
     * status. If the job is not yet in a globally terminal state, this method returns {@link
     * #UNKNOWN}.
     */
    public static ApplicationStatus fromJobStatus(JobStatus jobStatus) {
        if (jobStatus == null || !JOB_STATUS_APPLICATION_STATUS_BI_MAP.containsKey(jobStatus)) {
            return UNKNOWN;
        }

        return JOB_STATUS_APPLICATION_STATUS_BI_MAP.get(jobStatus);
    }

    public static JobStatus fromApplicationStatus(ApplicationStatus applicationStatus) {
        Preconditions.checkNotNull(applicationStatus, "ApplicationStatus must not be null");
        if (JOB_STATUS_APPLICATION_STATUS_BI_MAP.inverse().containsKey(applicationStatus)) {
            throw new UnsupportedOperationException(
                    applicationStatus.name() + " cannot be mapped to a JobStatus.");
        }

        return JOB_STATUS_APPLICATION_STATUS_BI_MAP.inverse().get(applicationStatus);
    }
}
