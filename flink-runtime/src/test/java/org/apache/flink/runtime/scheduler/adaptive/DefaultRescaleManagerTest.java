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

package org.apache.flink.runtime.scheduler.adaptive;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scheduler.adaptive.DefaultRescaleManager.Cooldown;
import org.apache.flink.runtime.scheduler.adaptive.DefaultRescaleManager.Idling;
import org.apache.flink.runtime.scheduler.adaptive.DefaultRescaleManager.Stabilized;
import org.apache.flink.runtime.scheduler.adaptive.DefaultRescaleManager.Stabilizing;
import org.apache.flink.runtime.scheduler.adaptive.allocator.VertexParallelism;
import org.apache.flink.util.ConfigurationException;
import org.apache.flink.util.Preconditions;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

class DefaultRescaleManagerTest {

    @Test
    void testProperConfiguration() throws ConfigurationException {
        final Duration cooldownTimeout = Duration.ofMillis(1337);
        final Duration resourceStabilizationTimeout = Duration.ofMillis(7331);
        final Duration maximumDelayForRescaleTrigger = Duration.ofMillis(4242);

        final Configuration configuration = new Configuration();
        configuration.set(JobManagerOptions.SCHEDULER_SCALING_INTERVAL_MIN, cooldownTimeout);
        configuration.set(
                JobManagerOptions.RESOURCE_STABILIZATION_TIMEOUT, resourceStabilizationTimeout);
        configuration.set(
                JobManagerOptions.MAXIMUM_DELAY_FOR_SCALE_TRIGGER, maximumDelayForRescaleTrigger);

        final DefaultRescaleManager testInstance =
                DefaultRescaleManager.Factory.fromSettings(
                                AdaptiveScheduler.Settings.of(configuration))
                        .create(TestingRescaleManagerContext.stableContext(), Instant.now());
        assertThat(testInstance.cooldownTimeout).isEqualTo(cooldownTimeout);
        assertThat(testInstance.resourceStabilizationTimeout)
                .isEqualTo(resourceStabilizationTimeout);
        assertThat(testInstance.maxTriggerDelay).isEqualTo(maximumDelayForRescaleTrigger);
    }

    @Test
    void triggerWithoutChangeEventNoopInCooldownPhase() {
        // the transition to idle state is expected to be scheduled
        triggerWithoutChangeEventNoop(
                TestingRescaleManagerContext::createTestInstanceInCooldownPhase, Cooldown.class);
    }

    @Test
    void triggerWithoutChangeEventNoopInIdlingPhase() {
        triggerWithoutChangeEventNoop(
                TestingRescaleManagerContext::createTestInstanceThatPassedCooldownPhase,
                Idling.class);
    }

    private void triggerWithoutChangeEventNoop(
            Function<TestingRescaleManagerContext, DefaultRescaleManager> testInstanceCreator,
            Class<? extends DefaultRescaleManager.State> expectedState) {
        final TestingRescaleManagerContext ctx =
                TestingRescaleManagerContext.stableContext().withDesiredRescaling();
        final DefaultRescaleManager testInstance = testInstanceCreator.apply(ctx);

        testInstance.onTrigger();

        assertStateWithoutRescale(ctx, testInstance, expectedState);
    }

    @Test
    void testRescaleRightAfterCooldown() {
        final TestingRescaleManagerContext ctx =
                TestingRescaleManagerContext.stableContext().withDesiredRescaling();
        final DefaultRescaleManager testInstance = ctx.createTestInstanceInCooldownPhase();

        assertStateWithoutRescale(ctx, testInstance, Cooldown.class);

        testInstance.onChange();
        testInstance.onTrigger();

        assertStateWithoutRescale(ctx, testInstance, Cooldown.class);

        ctx.transitionToInclusiveCooldownEnd();

        assertStateWithoutRescale(ctx, testInstance, Cooldown.class);

        testInstance.onTrigger();

        ctx.passTime(Duration.ofMillis(1));

        assertStateWithoutRescale(ctx, testInstance, Stabilizing.class);

        testInstance.onTrigger();

        assertThat(ctx.rescaleWasTriggered()).isTrue();
    }

    @Test
    void testDesiredChangeEventDuringCooldown() {
        final TestingRescaleManagerContext ctx =
                TestingRescaleManagerContext.stableContext().withDesiredRescaling();
        final DefaultRescaleManager testInstance = ctx.createTestInstanceInCooldownPhase();

        assertStateWithoutRescale(ctx, testInstance, Cooldown.class);

        testInstance.onChange();

        assertStateWithoutRescale(ctx, testInstance, Cooldown.class);

        testInstance.onTrigger();

        assertStateWithoutRescale(ctx, testInstance, Cooldown.class);

        ctx.transitionOutOfCooldownPhase();

        assertStateWithoutRescale(ctx, testInstance, Stabilizing.class);

        testInstance.onTrigger();

        assertStateWithRescale(ctx, testInstance);
    }

    @Test
    void testDesiredChangeEventInIdlingState() {
        final TestingRescaleManagerContext ctx =
                TestingRescaleManagerContext.stableContext().withDesiredRescaling();
        final DefaultRescaleManager testInstance = ctx.createTestInstanceThatPassedCooldownPhase();

        assertStateWithoutRescale(ctx, testInstance, Idling.class);

        testInstance.onChange();

        assertStateWithoutRescale(ctx, testInstance, Stabilizing.class);

        testInstance.onTrigger();

        assertStateWithRescale(ctx, testInstance);
    }

    @Test
    void testDesiredChangeEventInStabilizedState() {
        final TestingRescaleManagerContext ctx =
                TestingRescaleManagerContext.stableContext().withSufficientRescaling();
        final DefaultRescaleManager testInstance = ctx.createTestInstanceInStabilizedPhase();

        assertStateWithoutRescale(ctx, testInstance, Stabilized.class);

        ctx.withDesiredRescaling();

        testInstance.onChange();

        assertStateWithoutRescale(ctx, testInstance, Stabilized.class);

        testInstance.onTrigger();

        assertStateWithRescale(ctx, testInstance);
    }

    @Test
    void testNoRescaleInCooldownPhase() {
        final TestingRescaleManagerContext ctx = TestingRescaleManagerContext.stableContext();
        final DefaultRescaleManager testInstance = ctx.createTestInstanceInCooldownPhase();

        assertStateWithoutRescale(ctx, testInstance, Cooldown.class);

        testInstance.onChange();

        assertStateWithoutRescale(ctx, testInstance, Cooldown.class);

        testInstance.onTrigger();

        assertStateWithoutRescale(ctx, testInstance, Cooldown.class);

        ctx.transitionOutOfCooldownPhase();

        assertStateWithoutRescale(ctx, testInstance, Stabilizing.class);

        ctx.passResourceStabilizationTimeout();

        assertStateWithoutRescale(ctx, testInstance, Stabilized.class);

        testInstance.onTrigger();

        assertStateWithoutRescale(ctx, testInstance, Idling.class);
    }

    @Test
    void testNoRescaleInStabilizingState() {
        final TestingRescaleManagerContext ctx = TestingRescaleManagerContext.stableContext();
        final DefaultRescaleManager testInstance = ctx.createTestInstanceThatPassedCooldownPhase();

        assertStateWithoutRescale(ctx, testInstance, Idling.class);

        testInstance.onChange();

        assertStateWithoutRescale(ctx, testInstance, Stabilizing.class);

        testInstance.onTrigger();

        assertStateWithoutRescale(ctx, testInstance, Stabilizing.class);

        ctx.passResourceStabilizationTimeout();

        assertStateWithoutRescale(ctx, testInstance, Stabilized.class);

        testInstance.onTrigger();

        assertStateWithoutRescale(ctx, testInstance, Idling.class);
    }

    @Test
    void testNoResaleInStabilizedState() {
        final TestingRescaleManagerContext ctx = TestingRescaleManagerContext.stableContext();
        final DefaultRescaleManager testInstance = ctx.createTestInstanceInStabilizedPhase();

        assertStateWithoutRescale(ctx, testInstance, Stabilized.class);

        testInstance.onTrigger();

        assertStateWithoutRescale(ctx, testInstance, Idling.class);
    }

    @Test
    void testSufficientChangeInCooldownState() {
        final TestingRescaleManagerContext ctx =
                TestingRescaleManagerContext.stableContext().withSufficientRescaling();
        final DefaultRescaleManager testInstance = ctx.createTestInstanceInCooldownPhase();

        assertStateWithoutRescale(ctx, testInstance, Cooldown.class);

        testInstance.onChange();

        assertStateWithoutRescale(ctx, testInstance, Cooldown.class);

        testInstance.onTrigger();

        assertStateWithoutRescale(ctx, testInstance, Cooldown.class);

        ctx.transitionOutOfCooldownPhase();

        assertStateWithoutRescale(ctx, testInstance, Stabilizing.class);

        testInstance.onTrigger();

        assertStateWithoutRescale(ctx, testInstance, Stabilizing.class);

        ctx.passResourceStabilizationTimeout();

        assertStateWithoutRescale(ctx, testInstance, Stabilized.class);

        testInstance.onTrigger();

        assertStateWithRescale(ctx, testInstance);
    }

    @Test
    void testSufficientChangeInIdlingPhase() {
        final TestingRescaleManagerContext ctx =
                TestingRescaleManagerContext.stableContext().withSufficientRescaling();
        final DefaultRescaleManager testInstance = ctx.createTestInstanceThatPassedCooldownPhase();

        assertStateWithoutRescale(ctx, testInstance, Idling.class);

        testInstance.onChange();

        assertStateWithoutRescale(ctx, testInstance, Stabilizing.class);

        testInstance.onTrigger();

        assertStateWithoutRescale(ctx, testInstance, Stabilizing.class);

        ctx.passResourceStabilizationTimeout();

        assertStateWithoutRescale(ctx, testInstance, Stabilized.class);

        testInstance.onTrigger();

        assertStateWithRescale(ctx, testInstance);
    }

    @Test
    void testSufficientChangeInCooldownWithSubsequentDesiredChangeInStabilizingState() {
        final TestingRescaleManagerContext ctx =
                TestingRescaleManagerContext.stableContext().withSufficientRescaling();
        final DefaultRescaleManager testInstance = ctx.createTestInstanceInCooldownPhase();

        assertStateWithoutRescale(ctx, testInstance, Cooldown.class);

        testInstance.onChange();

        assertStateWithoutRescale(ctx, testInstance, Cooldown.class);

        testInstance.onTrigger();

        assertStateWithoutRescale(ctx, testInstance, Cooldown.class);

        ctx.transitionOutOfCooldownPhase();

        assertStateWithoutRescale(ctx, testInstance, Stabilizing.class);

        ctx.withDesiredRescaling();

        testInstance.onChange();

        assertStateWithoutRescale(ctx, testInstance, Stabilizing.class);

        testInstance.onTrigger();

        assertStateWithRescale(ctx, testInstance);
    }

    @Test
    void testSufficientChangeWithSubsequentDesiredChangeInStabilizingState() {
        final TestingRescaleManagerContext ctx =
                TestingRescaleManagerContext.stableContext().withSufficientRescaling();
        final DefaultRescaleManager testInstance = ctx.createTestInstanceThatPassedCooldownPhase();

        assertStateWithoutRescale(ctx, testInstance, Idling.class);

        testInstance.onChange();

        assertStateWithoutRescale(ctx, testInstance, Stabilizing.class);

        testInstance.onTrigger();

        assertStateWithoutRescale(ctx, testInstance, Stabilizing.class);

        ctx.withDesiredRescaling();

        testInstance.onChange();

        assertStateWithoutRescale(ctx, testInstance, Stabilizing.class);

        testInstance.onTrigger();

        assertThat(ctx.rescaleWasTriggered()).isTrue();
    }

    @Test
    void
            testRevokedSufficientChangeInStabilizingStateWithSubsequentSufficientChangeInStabilizedState() {
        final TestingRescaleManagerContext ctx =
                TestingRescaleManagerContext.stableContext().withSufficientRescaling();
        final DefaultRescaleManager testInstance = ctx.createTestInstanceThatPassedCooldownPhase();

        assertStateWithoutRescale(ctx, testInstance, Idling.class);

        testInstance.onChange();

        assertStateWithoutRescale(ctx, testInstance, Stabilizing.class);

        testInstance.onTrigger();

        assertStateWithoutRescale(ctx, testInstance, Stabilizing.class);

        ctx.revertAnyParallelismImprovements();

        testInstance.onChange();

        assertStateWithoutRescale(ctx, testInstance, Stabilizing.class);

        testInstance.onTrigger();

        assertStateWithoutRescale(ctx, testInstance, Stabilizing.class);

        ctx.passResourceStabilizationTimeout();

        assertStateWithoutRescale(ctx, testInstance, Stabilized.class);

        ctx.withSufficientRescaling();

        testInstance.onChange();

        assertStateWithoutRescale(ctx, testInstance, Stabilized.class);

        testInstance.onTrigger();

        assertStateWithRescale(ctx, testInstance);
    }

    @Test
    void testRevokedChangeInStabilizedStateResultsInTransitioningToIdleState() {
        final TestingRescaleManagerContext ctx = TestingRescaleManagerContext.stableContext();
        final DefaultRescaleManager testInstance = ctx.createTestInstanceInStabilizedPhase();

        assertStateWithoutRescale(ctx, testInstance, Stabilized.class);

        testInstance.onTrigger();

        assertStateWithoutRescale(ctx, testInstance, Idling.class);

        ctx.withSufficientRescaling();

        testInstance.onChange();

        assertStateWithoutRescale(ctx, testInstance, Stabilizing.class);

        testInstance.onTrigger();

        assertStateWithoutRescale(ctx, testInstance, Stabilizing.class);

        ctx.withDesiredRescaling();

        assertStateWithoutRescale(ctx, testInstance, Stabilizing.class);

        testInstance.onTrigger();

        assertStateWithRescale(ctx, testInstance);
    }

    private static void assertStateWithoutRescale(
            TestingRescaleManagerContext ctx,
            DefaultRescaleManager testInstance,
            Class<? extends DefaultRescaleManager.State> expectedState) {
        assertThat(ctx.rescaleWasTriggered()).isFalse();
        assertThat(testInstance.getState()).isInstanceOf(expectedState);
    }

    private static void assertStateWithRescale(
            TestingRescaleManagerContext ctx, DefaultRescaleManager testInstance) {
        assertThat(ctx.rescaleWasTriggered()).isTrue();
        assertThat(testInstance.getState()).isNull();
    }

    /**
     * {@code TestingRescaleManagerContext} provides methods for adjusting the elapsed time and for
     * adjusting the available resources for rescaling.
     */
    private static class TestingRescaleManagerContext implements RescaleManager.Context {

        private static final JobVertexID JOB_VERTEX_ID = new JobVertexID();

        // default configuration values to allow for easy transitioning between the phases
        private static final Duration COOLDOWN_TIMEOUT = Duration.ofHours(1);
        private static final Duration RESOURCE_STABILIZATION_TIMEOUT = Duration.ofHours(2);
        private static final int MIN_PARALLELISM_CHANGE = 2;

        private static final int CURRENT_PARALLELISM = 1;

        // configuration that defines what kind of rescaling would be possible
        private boolean hasSufficientResources = false;
        private boolean hasDesiredResources = false;

        // internal state used for assertions
        private final AtomicBoolean rescaleTriggered = new AtomicBoolean();
        private final SortedMap<Instant, List<Runnable>> scheduledTasks = new TreeMap<>();

        // Instant.MIN makes debugging easier because timestamps become human-readable
        private final Instant initializationTime = Instant.MIN;
        private Duration elapsedTime = Duration.ZERO;

        private static VertexParallelism createVertexParallelism(int parallelism) {
            final Map<JobVertexID, Integer> map = new HashMap<>();
            map.put(JOB_VERTEX_ID, parallelism);

            return new VertexParallelism(map);
        }

        // ///////////////////////////////////////////////
        // Context creation
        // ///////////////////////////////////////////////

        public static TestingRescaleManagerContext stableContext() {
            return new TestingRescaleManagerContext();
        }

        private TestingRescaleManagerContext() {
            // no rescaling is enabled by default
            revertAnyParallelismImprovements();
        }

        public void revertAnyParallelismImprovements() {
            this.hasSufficientResources = false;
            this.hasDesiredResources = false;
        }

        public TestingRescaleManagerContext withDesiredRescaling() {
            // having desired resources should also mean that the sufficient resources are met
            this.hasSufficientResources = true;
            this.hasDesiredResources = true;

            return this;
        }

        public TestingRescaleManagerContext withSufficientRescaling() {
            this.hasSufficientResources = true;
            this.hasDesiredResources = false;

            return this;
        }

        // ///////////////////////////////////////////////
        // RescaleManager.Context interface methods
        // ///////////////////////////////////////////////

        @Override
        public boolean hasSufficientResources() {
            return this.hasSufficientResources;
        }

        @Override
        public boolean hasDesiredResources() {
            return this.hasDesiredResources;
        }

        @Override
        public void rescale() {
            rescaleTriggered.set(true);
        }

        @Override
        public void scheduleOperation(Runnable callback, Duration delay) {
            final Instant triggerTime =
                    Objects.requireNonNull(initializationTime).plus(elapsedTime).plus(delay);
            if (!scheduledTasks.containsKey(triggerTime)) {
                scheduledTasks.put(triggerTime, new ArrayList<>());
            }

            scheduledTasks.get(triggerTime).add(callback);
        }

        // ///////////////////////////////////////////////
        // Test instance creation
        // ///////////////////////////////////////////////

        /**
         * Creates the {@code DefaultRescaleManager} test instance and transitions into a period in
         * time where the instance is in cooldown phase.
         */
        public DefaultRescaleManager createTestInstanceInCooldownPhase() {
            return createTestInstance(
                    ignoredRescaleManager -> this.transitionIntoCooldownTimeframe());
        }

        /**
         * Creates the {@code DefaultRescaleManager} test instance and transitions into a period in
         * time where the instance is in soft-rescaling phase.
         */
        public DefaultRescaleManager createTestInstanceThatPassedCooldownPhase() {
            return createTestInstance(ignoredResaleManager -> this.transitionOutOfCooldownPhase());
        }

        /**
         * Creates the {@code DefaultRescaleManager} test instance and transitions into a period in
         * time where the instance is in hard-rescaling phase.
         */
        public DefaultRescaleManager createTestInstanceInStabilizedPhase() {
            return createTestInstance(
                    rescaleManager -> {
                        rescaleManager.onChange();
                        passResourceStabilizationTimeout();
                    });
        }

        /**
         * Initializes the test instance and sets the context's elapsed time based on the passed
         * callback.
         */
        private DefaultRescaleManager createTestInstance(Consumer<DefaultRescaleManager> callback) {
            final DefaultRescaleManager testInstance =
                    new DefaultRescaleManager(
                            initializationTime,
                            // clock that returns the time based on the configured elapsedTime
                            () -> Objects.requireNonNull(initializationTime).plus(elapsedTime),
                            this,
                            COOLDOWN_TIMEOUT,
                            RESOURCE_STABILIZATION_TIMEOUT,
                            Duration.ofHours(5)) {
                        @Override
                        public void onChange() {
                            super.onChange();

                            // hack to avoid calling this method in every test method
                            // we want to trigger tasks that are meant to run right-away
                            TestingRescaleManagerContext.this.triggerOutdatedTasks();
                        }

                        @Override
                        public void onTrigger() {
                            super.onTrigger();

                            // hack to avoid calling this method in every test method
                            // we want to trigger tasks that are meant to run right-away
                            TestingRescaleManagerContext.this.triggerOutdatedTasks();
                        }
                    };

            callback.accept(testInstance);

            return testInstance;
        }

        // ///////////////////////////////////////////////
        // Time-adjustment functionality
        // ///////////////////////////////////////////////

        public void transitionIntoCooldownTimeframe() {
            setElapsedTime(COOLDOWN_TIMEOUT.dividedBy(2));
            this.triggerOutdatedTasks();
        }

        public void transitionToInclusiveCooldownEnd() {
            setElapsedTime(COOLDOWN_TIMEOUT.minusMillis(1));
        }

        public void passTime(Duration elapsed) {
            setElapsedTime(this.elapsedTime.plus(elapsed));
        }

        public void setElapsedTime(Duration elapsedTime) {
            Preconditions.checkState(
                    this.elapsedTime.compareTo(elapsedTime) <= 0,
                    "The elapsed time should monotonically increase.");
            this.elapsedTime = elapsedTime;
            this.triggerOutdatedTasks();
        }

        public void transitionOutOfCooldownPhase() {
            this.setElapsedTime(COOLDOWN_TIMEOUT.plusMillis(1));
        }

        public void passResourceStabilizationTimeout() {
            // resource stabilization is based on the current time
            this.passTime(RESOURCE_STABILIZATION_TIMEOUT.plusMillis(1));
        }

        private void triggerOutdatedTasks() {
            while (!scheduledTasks.isEmpty()) {
                final Instant timeOfExecution = scheduledTasks.firstKey();
                if (!timeOfExecution.isAfter(
                        Objects.requireNonNull(initializationTime).plus(elapsedTime))) {
                    scheduledTasks.remove(timeOfExecution).forEach(Runnable::run);
                } else {
                    break;
                }
            }
        }

        // ///////////////////////////////////////////////
        // Methods for verifying the context's state
        // ///////////////////////////////////////////////

        public boolean rescaleWasTriggered() {
            return rescaleTriggered.get();
        }
    }
}
