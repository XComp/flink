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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code CheckpointRecoveryFactoryTest} tests the default functionality of {@link
 * CheckpointRecoveryFactory}.
 */
public class CheckpointRecoveryFactoryTest {

    private static final Logger log = LoggerFactory.getLogger(CheckpointRecoveryFactoryTest.class);

    @ParameterizedTest(name = "actual: {0}; expected: {1}")
    @CsvSource({"10,10", "0,1", "-1,1"})
    public void testMaxRemainingCheckpointsParameterSetting(int actualValue, int expectedValue)
            throws Exception {
        final JobID expectedJobId = new JobID();
        final Configuration jobManagerConfig = new Configuration();
        jobManagerConfig.setInteger(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS, actualValue);

        final TestCheckpointRecoveryFactory testInstance = new TestCheckpointRecoveryFactory();
        assertThat(
                        testInstance.createRecoveredCompletedCheckpointStore(
                                expectedJobId, jobManagerConfig, log))
                .isNull();

        assertThat(testInstance.getActualJobId()).isEqualTo(expectedJobId);
        assertThat(testInstance.getActualMaximumNumberOfRetainedCheckpointsParamValue())
                .isEqualTo(expectedValue);
    }

    private static class TestCheckpointRecoveryFactory implements CheckpointRecoveryFactory {

        private JobID actualJobId;
        private int actualMaximumNumberOfRetainedCheckpointsParamValue;

        @Nullable
        @Override
        public CompletedCheckpointStore createRecoveredCompletedCheckpointStore(
                JobID jobId, int maxNumberOfCheckpointsToRetain) throws Exception {
            this.actualJobId = jobId;
            this.actualMaximumNumberOfRetainedCheckpointsParamValue =
                    maxNumberOfCheckpointsToRetain;

            return null;
        }

        @Override
        public CheckpointIDCounter createCheckpointIDCounter(JobID jobId) throws Exception {
            throw new UnsupportedOperationException("createCheckpointIDCounter is not implemented");
        }

        public JobID getActualJobId() {
            return actualJobId;
        }

        public int getActualMaximumNumberOfRetainedCheckpointsParamValue() {
            return actualMaximumNumberOfRetainedCheckpointsParamValue;
        }
    }
}
