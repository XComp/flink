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

package org.apache.flink.runtime.leaderelection;

import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Test {@link LeaderElectionService} implementation which directly forwards isLeader and notLeader
 * calls to the contender.
 */
public class TestingLeaderElectionService implements LeaderElectionService {

    @Nullable private TestingLeaderElection startedLeaderElection;

    @Override
    public synchronized LeaderElection createLeaderElection() {
        startedLeaderElection = new TestingLeaderElection();
        return startedLeaderElection;
    }

    @Override
    public synchronized void stop() throws Exception {
        Preconditions.checkState(startedLeaderElection != null, "No LeaderElection created, yet.");

        startedLeaderElection = null;
    }

    public synchronized CompletableFuture<UUID> isLeader(UUID leaderSessionID) {
        Preconditions.checkState(startedLeaderElection != null, "No LeaderElection created, yet.");

        return startedLeaderElection
                .isLeader(leaderSessionID)
                .thenApply(LeaderInformation::getLeaderSessionID);
    }

    public synchronized void notLeader() {
        Preconditions.checkState(startedLeaderElection != null, "No LeaderElection created, yet.");

        startedLeaderElection.notLeader();
    }

    public synchronized String getAddress() {
        Preconditions.checkState(startedLeaderElection != null, "No LeaderElection created, yet.");

        final CompletableFuture<LeaderInformation> confirmedLeaderInformation =
                startedLeaderElection.getConfirmedLeaderInformation();

        Preconditions.checkState(
                confirmedLeaderInformation != null, "The leadership wasn't acquired, yet.");

        if (confirmedLeaderInformation.isDone()) {
            return confirmedLeaderInformation.join().getLeaderAddress();
        } else {
            throw new IllegalStateException("The leadership wasn't confirmed, yet.");
        }
    }

    /**
     * Returns the start future indicating whether this leader election service has been started or
     * not.
     *
     * @return Future which is completed once this service has been started
     */
    public synchronized CompletableFuture<Void> getStartFuture() {
        Preconditions.checkState(startedLeaderElection != null, "No LeaderElection created, yet.");

        return startedLeaderElection.getStartFuture();
    }

    public synchronized boolean isStopped() {
        Preconditions.checkState(startedLeaderElection != null, "No LeaderElection created, yet.");

        return startedLeaderElection.isStopped();
    }
}
