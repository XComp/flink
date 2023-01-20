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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.util.UUID;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Default implementation for leader election service. Composed with different {@link
 * LeaderElectionDriver}, we could perform a leader election for the contender, and then persist the
 * leader information to various storage.
 */
public class DefaultLeaderElectionService
        implements LeaderElectionService, LeaderElectionEventHandler {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultLeaderElectionService.class);

    private final Object lock = new Object();

    private final LeaderElectionDriverFactory leaderElectionDriverFactory;

    /** The leader contender which applies for leadership. */
    // this.running=true ensures that a contender is set
    @GuardedBy("lock")
    @Nullable
    private volatile LeaderContender leaderContender;

    @GuardedBy("lock")
    @Nullable
    private volatile UUID issuedLeaderSessionID;

    @GuardedBy("lock")
    private volatile LeaderInformation confirmedLeaderInformation;

    @GuardedBy("lock")
    private volatile boolean running;

    // this.running=true ensures that a contender is set
    @Nullable private LeaderElectionDriver leaderElectionDriver;

    public DefaultLeaderElectionService(LeaderElectionDriverFactory leaderElectionDriverFactory) {
        this.leaderElectionDriverFactory = checkNotNull(leaderElectionDriverFactory);

        this.leaderContender = null;

        this.issuedLeaderSessionID = null;

        this.leaderElectionDriver = null;

        this.confirmedLeaderInformation = LeaderInformation.empty();

        this.running = false;
    }

    @Override
    public final void start(LeaderContender contender) throws Exception {
        checkNotNull(contender, "Contender must not be null.");
        Preconditions.checkState(leaderContender == null, "Contender was already set.");

        synchronized (lock) {
            running = true;
            leaderContender = contender;
            leaderElectionDriver =
                    leaderElectionDriverFactory.createLeaderElectionDriver(
                            this,
                            new LeaderElectionFatalErrorHandler(),
                            contender.getDescription());
            LOG.info("Starting DefaultLeaderElectionService with {}.", leaderElectionDriver);
        }
    }

    @Override
    public final void stop() throws Exception {
        LOG.info("Stopping DefaultLeaderElectionService.");

        synchronized (lock) {
            if (!running) {
                return;
            }
            running = false;
            clearConfirmedLeaderInformation();
        }

        checkNotNull(leaderElectionDriver, "LeaderElectionDriver is guarded by the running flag.")
                .close();
    }

    @Override
    public void confirmLeadership(UUID leaderSessionID, String leaderAddress) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Confirm leader session ID {} for leader {}.", leaderSessionID, leaderAddress);
        }

        checkNotNull(leaderSessionID);

        synchronized (lock) {
            if (hasLeadership(leaderSessionID)) {
                if (running) {
                    confirmLeaderInformation(leaderSessionID, leaderAddress);
                } else {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(
                                "Ignoring the leader session Id {} confirmation, since the "
                                        + "LeaderElectionService has already been stopped.",
                                leaderSessionID);
                    }
                }
            } else {
                // Received an old confirmation call
                if (!leaderSessionID.equals(this.issuedLeaderSessionID)) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(
                                "Receive an old confirmation call of leader session ID {}, "
                                        + "current issued session ID is {}",
                                leaderSessionID,
                                issuedLeaderSessionID);
                    }
                } else {
                    LOG.warn(
                            "The leader session ID {} was confirmed even though the "
                                    + "corresponding JobManager was not elected as the leader.",
                            leaderSessionID);
                }
            }
        }
    }

    @Override
    public boolean hasLeadership(@Nonnull UUID leaderSessionId) {
        synchronized (lock) {
            if (running) {
                return getLeaderElectionDriverGuardedByRunningState().hasLeadership()
                        && leaderSessionId.equals(issuedLeaderSessionID);
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "hasLeadership is called after the service is stopped, returning false.");
                }
                return false;
            }
        }
    }

    /**
     * Returns the current leader session ID or null, if the contender is not the leader.
     *
     * @return The last leader session ID or null, if the contender is not the leader
     */
    @VisibleForTesting
    @Nullable
    public UUID getLeaderSessionID() {
        return confirmedLeaderInformation.getLeaderSessionID();
    }

    @GuardedBy("lock")
    private void confirmLeaderInformation(UUID leaderSessionID, String leaderAddress) {
        confirmedLeaderInformation = LeaderInformation.known(leaderSessionID, leaderAddress);
        getLeaderElectionDriverGuardedByRunningState()
                .writeLeaderInformation(confirmedLeaderInformation);
    }

    @GuardedBy("lock")
    private void clearConfirmedLeaderInformation() {
        confirmedLeaderInformation = LeaderInformation.empty();
    }

    @Override
    public void onGrantLeadership(UUID newLeaderSessionId) {
        synchronized (lock) {
            if (running) {
                issuedLeaderSessionID = newLeaderSessionId;
                clearConfirmedLeaderInformation();

                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "Grant leadership to contender {} with session ID {}.",
                            getLeaderContenderGuardedByRunningState().getDescription(),
                            issuedLeaderSessionID);
                }

                getLeaderContenderGuardedByRunningState().grantLeadership(issuedLeaderSessionID);
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "Ignoring the grant leadership notification since the {} has "
                                    + "already been closed.",
                            leaderElectionDriver);
                }
            }
        }
    }

    @Override
    public void onRevokeLeadership() {
        synchronized (lock) {
            if (running) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "Revoke leadership of {} ({}@{}).",
                            getLeaderContenderGuardedByRunningState().getDescription(),
                            confirmedLeaderInformation.getLeaderSessionID(),
                            confirmedLeaderInformation.getLeaderAddress());
                }

                issuedLeaderSessionID = null;
                clearConfirmedLeaderInformation();

                getLeaderContenderGuardedByRunningState().revokeLeadership();

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Clearing the leader information on {}.", leaderElectionDriver);
                }
                // Clear the old leader information on the external storage
                getLeaderElectionDriverGuardedByRunningState()
                        .writeLeaderInformation(LeaderInformation.empty());
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "Ignoring the revoke leadership notification since the {} "
                                    + "has already been closed.",
                            leaderElectionDriver);
                }
            }
        }
    }

    @Override
    public void onLeaderInformationChange(LeaderInformation leaderInformation) {
        synchronized (lock) {
            if (running) {
                if (LOG.isTraceEnabled()) {
                    LOG.trace(
                            "Leader node changed while {} is the leader with session ID {}. New leader information {}.",
                            getLeaderContenderGuardedByRunningState().getDescription(),
                            confirmedLeaderInformation.getLeaderSessionID(),
                            leaderInformation);
                }
                if (!confirmedLeaderInformation.isEmpty()) {
                    final LeaderInformation confirmedLeaderInfo = this.confirmedLeaderInformation;
                    if (leaderInformation.isEmpty()) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug(
                                    "Writing leader information by {} since the external storage is empty.",
                                    getLeaderContenderGuardedByRunningState().getDescription());
                        }
                        getLeaderElectionDriverGuardedByRunningState()
                                .writeLeaderInformation(confirmedLeaderInfo);
                    } else if (!leaderInformation.equals(confirmedLeaderInfo)) {
                        // the data field does not correspond to the expected leader information
                        if (LOG.isDebugEnabled()) {
                            LOG.debug(
                                    "Correcting leader information by {}.",
                                    getLeaderContenderGuardedByRunningState().getDescription());
                        }
                        getLeaderElectionDriverGuardedByRunningState()
                                .writeLeaderInformation(confirmedLeaderInfo);
                    }
                }
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "Ignoring change notification since the {} has "
                                    + "already been closed.",
                            leaderElectionDriver);
                }
            }
        }
    }

    /**
     * Returns {@link #leaderElectionDriver} with proper state checks.
     *
     * @throws IllegalStateException if this method is called without this instance being in state
     *     running.
     * @see #running
     */
    private LeaderElectionDriver getLeaderElectionDriverGuardedByRunningState() {
        Preconditions.checkState(
                running, "LeaderElectionDriver should only be requested if in running state.");
        return checkNotNull(
                leaderElectionDriver,
                "The LeaderElectionDriver should be set if in state running.");
    }

    /**
     * Returns {@link #leaderContender} with proper state checks.
     *
     * @throws IllegalStateException if this method is called without this instance being in state
     *     running.
     * @see #running
     */
    private LeaderContender getLeaderContenderGuardedByRunningState() {
        Preconditions.checkState(
                running, "LeaderContender should only be requested if in running state.");
        return checkNotNull(
                leaderContender, "The LeaderContender should be set if in state running.");
    }

    private class LeaderElectionFatalErrorHandler implements FatalErrorHandler {

        @Override
        public void onFatalError(Throwable throwable) {
            synchronized (lock) {
                if (!running) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(
                                "Ignoring error notification since the service has been stopped.");
                    }
                    return;
                }

                if (throwable instanceof LeaderElectionException) {
                    getLeaderContenderGuardedByRunningState()
                            .handleError((LeaderElectionException) throwable);
                } else {
                    getLeaderContenderGuardedByRunningState()
                            .handleError(new LeaderElectionException(throwable));
                }
            }
        }
    }
}
