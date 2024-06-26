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
                TestingRescaleManagerContext::createTestInstanceInCooldownPhase, true);
    }

    @Test
    void triggerWithoutChangeEventNoopInSoftRescalingPhase() {
        triggerWithoutChangeEventNoop(
                TestingRescaleManagerContext::createTestInstanceInSoftRescalePhase, false);
    }

    @Test
    void triggerWithoutChangeEventNoopInHardRescalingPhase() {
        triggerWithoutChangeEventNoop(
                TestingRescaleManagerContext::createTestInstanceInHardRescalePhase, false);
    }

    private void triggerWithoutChangeEventNoop(
            Function<TestingRescaleManagerContext, DefaultRescaleManager> testInstanceCreator,
            boolean expectedScheduledTasks) {
        final TestingRescaleManagerContext ctx =
                TestingRescaleManagerContext.stableContext().withDesiredRescaling();
        final DefaultRescaleManager testInstance = testInstanceCreator.apply(ctx);

        testInstance.onTrigger();

        assertThat(ctx.rescaleWasTriggered())
                .as(
                        "No rescaling should have been triggered due to the missing change event despite the fact that desired rescaling would be possible.")
                .isFalse();
        assertThat(ctx.additionalTasksWaiting())
                .as("No tasks should be scheduled.")
                .isEqualTo(expectedScheduledTasks);
    }

    @Test
    void testRescaleRightAfterCooldown() {
        final TestingRescaleManagerContext ctx =
                TestingRescaleManagerContext.stableContext().withDesiredRescaling();
        final DefaultRescaleManager testInstance = ctx.createTestInstanceInCooldownPhase();

        testInstance.onChange();
        testInstance.onTrigger();

        assertThat(ctx.rescaleWasTriggered())
                .as("Rescaling wasn't triggered, yet, because we're still in cooldown phase.")
                .isFalse();
        assertThat(ctx.additionalTasksWaiting()).isTrue();

        ctx.transitionToInclusiveCooldownEnd();

        assertThat(ctx.rescaleWasTriggered())
                .as("Rescaling wasn't triggered, yet, because we're still in cooldown phase.")
                .isFalse();
        assertThat(ctx.additionalTasksWaiting()).isTrue();

        ctx.passTime(Duration.ofMillis(1));

        testInstance.onTrigger();

        assertThat(ctx.rescaleWasTriggered()).isTrue();
    }

    @Test
    void testDesiredChangeEventDuringCooldown() {
        final TestingRescaleManagerContext softScalePossibleCtx =
                TestingRescaleManagerContext.stableContext().withDesiredRescaling();
        final DefaultRescaleManager testInstance =
                softScalePossibleCtx.createTestInstanceInCooldownPhase();

        testInstance.onChange();

        assertIntermediateStateWithoutRescale(softScalePossibleCtx);

        testInstance.onTrigger();

        assertIntermediateStateWithoutRescale(softScalePossibleCtx);

        softScalePossibleCtx.transitionIntoSoftScalingTimeframe();

        testInstance.onTrigger();

        assertFinalStateWithRescale(softScalePossibleCtx);
    }

    @Test
    void testDesiredChangeEventInSoftRescalePhase() {
        final TestingRescaleManagerContext desiredRescalePossibleCtx =
                TestingRescaleManagerContext.stableContext().withDesiredRescaling();
        final DefaultRescaleManager testInstance =
                desiredRescalePossibleCtx.createTestInstanceInSoftRescalePhase();

        testInstance.onChange();

        assertIntermediateStateWithoutRescale(desiredRescalePossibleCtx);

        testInstance.onTrigger();

        assertFinalStateWithRescale(desiredRescalePossibleCtx);
    }

    @Test
    void testDesiredChangeEventInHardRescalePhase() {
        final TestingRescaleManagerContext desiredRescalePossibleCtx =
                TestingRescaleManagerContext.stableContext().withDesiredRescaling();
        final DefaultRescaleManager testInstance =
                desiredRescalePossibleCtx.createTestInstanceInHardRescalePhase();

        testInstance.onChange();

        assertIntermediateStateWithoutRescale(desiredRescalePossibleCtx);

        testInstance.onTrigger();

        assertFinalStateWithRescale(desiredRescalePossibleCtx);
    }

    @Test
    void testNoRescaleInCooldownPhase() {
        final TestingRescaleManagerContext noRescalePossibleCtx =
                TestingRescaleManagerContext.stableContext();
        final DefaultRescaleManager testInstance =
                noRescalePossibleCtx.createTestInstanceInCooldownPhase();

        testInstance.onChange();

        assertIntermediateStateWithoutRescale(noRescalePossibleCtx);

        testInstance.onTrigger();

        assertIntermediateStateWithoutRescale(noRescalePossibleCtx);

        noRescalePossibleCtx.transitionIntoSoftScalingTimeframe();

        assertIntermediateStateWithoutRescale(noRescalePossibleCtx);

        noRescalePossibleCtx.transitionIntoHardScalingTimeframe();

        assertThat(noRescalePossibleCtx.rescaleWasTriggered())
                .as("No rescaling should have happened even in the hard-rescaling phase.")
                .isFalse();
        assertThat(noRescalePossibleCtx.additionalTasksWaiting())
                .as("No further tasks should have been waiting for execution.")
                .isTrue();
    }

    @Test
    void testNoRescaleInSoftRescalePhase() {
        final TestingRescaleManagerContext noRescalePossibleCtx =
                TestingRescaleManagerContext.stableContext();
        final DefaultRescaleManager testInstance =
                noRescalePossibleCtx.createTestInstanceInSoftRescalePhase();

        testInstance.onChange();

        assertIntermediateStateWithoutRescale(noRescalePossibleCtx);

        testInstance.onTrigger();

        assertIntermediateStateWithoutRescale(noRescalePossibleCtx);

        noRescalePossibleCtx.transitionIntoHardScalingTimeframe();

        assertThat(noRescalePossibleCtx.rescaleWasTriggered())
                .as("No rescaling should have happened even in the hard-rescaling phase.")
                .isFalse();
        assertThat(noRescalePossibleCtx.additionalTasksWaiting())
                .as("No further tasks should have been waiting for execution.")
                .isTrue();
    }

    @Test
    void testNoResaleInHardRescalePhase() {
        final TestingRescaleManagerContext noRescalePossibleCtx =
                TestingRescaleManagerContext.stableContext();
        final DefaultRescaleManager testInstance =
                noRescalePossibleCtx.createTestInstanceInHardRescalePhase();

        testInstance.onChange();

        assertIntermediateStateWithoutRescale(noRescalePossibleCtx);

        testInstance.onTrigger();

        assertThat(noRescalePossibleCtx.rescaleWasTriggered())
                .as("No rescaling should have happened even in the hard-rescaling phase.")
                .isFalse();
        assertThat(noRescalePossibleCtx.additionalTasksWaiting())
                .as("No further tasks should have been waiting for execution.")
                .isTrue();
    }

    @Test
    void testSufficientChangeInCooldownPhase() {
        final TestingRescaleManagerContext hardRescalePossibleCtx =
                TestingRescaleManagerContext.stableContext().withSufficientRescaling();
        final DefaultRescaleManager testInstance =
                hardRescalePossibleCtx.createTestInstanceInCooldownPhase();

        testInstance.onChange();

        assertIntermediateStateWithoutRescale(hardRescalePossibleCtx);

        testInstance.onTrigger();

        assertIntermediateStateWithoutRescale(hardRescalePossibleCtx);

        hardRescalePossibleCtx.transitionIntoSoftScalingTimeframe();

        testInstance.onTrigger();

        assertIntermediateStateWithoutRescale(hardRescalePossibleCtx);

        hardRescalePossibleCtx.transitionIntoHardScalingTimeframe();

        testInstance.onTrigger();

        assertFinalStateWithRescale(hardRescalePossibleCtx);
    }

    @Test
    void testSufficientChangeInSoftRescalePhase() {
        final TestingRescaleManagerContext hardRescalePossibleCtx =
                TestingRescaleManagerContext.stableContext().withSufficientRescaling();
        final DefaultRescaleManager testInstance =
                hardRescalePossibleCtx.createTestInstanceInSoftRescalePhase();

        testInstance.onChange();

        assertIntermediateStateWithoutRescale(hardRescalePossibleCtx);

        testInstance.onTrigger();

        assertIntermediateStateWithoutRescale(hardRescalePossibleCtx);

        hardRescalePossibleCtx.transitionIntoHardScalingTimeframe();

        testInstance.onTrigger();

        assertFinalStateWithRescale(hardRescalePossibleCtx);
    }

    @Test
    void testSufficientChangeInCooldownWithSubsequentDesiredChangeInSoftRescalePhase() {
        final TestingRescaleManagerContext ctx =
                TestingRescaleManagerContext.stableContext().withSufficientRescaling();
        final DefaultRescaleManager testInstance = ctx.createTestInstanceInCooldownPhase();

        testInstance.onChange();

        assertIntermediateStateWithoutRescale(ctx);

        testInstance.onTrigger();

        assertIntermediateStateWithoutRescale(ctx);

        ctx.transitionIntoSoftScalingTimeframe();

        ctx.withDesiredRescaling();

        testInstance.onChange();

        assertIntermediateStateWithoutRescale(ctx);

        testInstance.onTrigger();

        assertThat(ctx.rescaleWasTriggered()).isTrue();
        assertThat(ctx.numberOfTasksWaiting())
                .as("There should be a task scheduled to transition to stabilized state.")
                .isEqualTo(1);
    }

    @Test
    void testSufficientChangeWithSubsequentDesiredChangeInSoftRescalePhase() {
        final TestingRescaleManagerContext ctx =
                TestingRescaleManagerContext.stableContext().withSufficientRescaling();
        final DefaultRescaleManager testInstance = ctx.createTestInstanceInSoftRescalePhase();

        testInstance.onChange();

        assertIntermediateStateWithoutRescale(ctx);

        testInstance.onTrigger();

        assertIntermediateStateWithoutRescale(ctx);

        assertThat(ctx.numberOfTasksWaiting())
                .as(
                        "There should be a task scheduled that allows transitioning into stabilized state.")
                .isEqualTo(1);

        ctx.withDesiredRescaling();

        testInstance.onChange();

        assertIntermediateStateWithoutRescale(ctx);

        testInstance.onTrigger();

        assertThat(ctx.rescaleWasTriggered()).isTrue();
    }

    @Test
    void
            testRevokedSufficientChangeInSoftRescalePhaseWithSubsequentSufficientChangeInHardRescalingPhase() {
        final TestingRescaleManagerContext ctx =
                TestingRescaleManagerContext.stableContext().withSufficientRescaling();
        final DefaultRescaleManager testInstance = ctx.createTestInstanceInSoftRescalePhase();

        testInstance.onChange();

        assertIntermediateStateWithoutRescale(ctx);

        testInstance.onTrigger();

        assertIntermediateStateWithoutRescale(ctx);

        assertThat(ctx.numberOfTasksWaiting())
                .as(
                        "There should be a task scheduled that allows transitioning into hard-rescaling phase.")
                .isEqualTo(1);

        ctx.revertAnyParallelismImprovements();

        testInstance.onChange();

        assertIntermediateStateWithoutRescale(ctx);

        testInstance.onTrigger();

        assertIntermediateStateWithoutRescale(ctx);

        assertThat(ctx.numberOfTasksWaiting())
                .as(
                        "There should be a task scheduled that allows transitioning into hard-rescaling phase.")
                .isEqualTo(1);

        ctx.transitionIntoHardScalingTimeframe();

        assertThat(ctx.rescaleWasTriggered())
                .as(
                        "No rescaling should have been triggered because of the previous revert of the additional resources.")
                .isFalse();
        assertThat(ctx.additionalTasksWaiting())
                .as(
                        "The transition to hard-rescaling should have happened without any additional tasks in waiting state.")
                .isTrue();

        ctx.withSufficientRescaling();

        testInstance.onChange();

        assertIntermediateStateWithoutRescale(ctx);

        testInstance.onTrigger();

        assertFinalStateWithRescale(ctx);
    }

    @Test
    void testRevokedChangeInHardRescalingPhaseResultsInTransitioningToIdleState() {
        final TestingRescaleManagerContext ctx = TestingRescaleManagerContext.stableContext();
        final DefaultRescaleManager testInstance = ctx.createTestInstanceInHardRescalePhase();

        testInstance.onChange();

        assertIntermediateStateWithoutRescale(ctx);

        testInstance.onTrigger();

        assertThat(ctx.rescaleWasTriggered()).isFalse();
        assertThat(ctx.additionalTasksWaiting()).isTrue();

        ctx.withSufficientRescaling();

        testInstance.onChange();

        assertIntermediateStateWithoutRescale(ctx);

        testInstance.onTrigger();

        assertThat(ctx.rescaleWasTriggered()).isFalse();
        assertThat(ctx.additionalTasksWaiting()).isTrue();

        ctx.withDesiredRescaling();

        assertIntermediateStateWithoutRescale(ctx);

        testInstance.onTrigger();

        assertFinalStateWithRescale(ctx);
    }

    private static void assertIntermediateStateWithoutRescale(TestingRescaleManagerContext ctx) {
        assertThat(ctx.rescaleWasTriggered())
                .as("The rescale should not have been triggered, yet.")
                .isFalse();
        assertThat(ctx.additionalTasksWaiting())
                .as("There should be still tasks being scheduled.")
                .isTrue();
    }

    private static void assertFinalStateWithRescale(TestingRescaleManagerContext ctx) {
        assertThat(ctx.rescaleWasTriggered())
                .as("The rescale should have been triggered already.")
                .isTrue();
        assertThat(ctx.additionalTasksWaiting())
                .as("All scheduled tasks should have been executed.")
                .isTrue();
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
            return createTestInstance(this::transitionIntoCooldownTimeframe);
        }

        /**
         * Creates the {@code DefaultRescaleManager} test instance and transitions into a period in
         * time where the instance is in soft-rescaling phase.
         */
        public DefaultRescaleManager createTestInstanceInSoftRescalePhase() {
            return createTestInstance(this::transitionIntoSoftScalingTimeframe);
        }

        /**
         * Creates the {@code DefaultRescaleManager} test instance and transitions into a period in
         * time where the instance is in hard-rescaling phase.
         */
        public DefaultRescaleManager createTestInstanceInHardRescalePhase() {
            return createTestInstance(this::transitionIntoHardScalingTimeframe);
        }

        /**
         * Initializes the test instance and sets the context's elapsed time based on the passed
         * callback.
         */
        private DefaultRescaleManager createTestInstance(Runnable timeTransitioning) {
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

            timeTransitioning.run();
            return testInstance;
        }

        // ///////////////////////////////////////////////
        // Time-adjustment functionality
        // ///////////////////////////////////////////////

        /**
         * Transitions the context's time to a moment that falls into the test instance's cooldown
         * phase.
         */
        public void transitionIntoCooldownTimeframe() {
            this.elapsedTime = COOLDOWN_TIMEOUT.dividedBy(2);
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

        /**
         * Transitions the context's time to a moment that falls into the test instance's
         * soft-scaling phase.
         */
        public void transitionIntoSoftScalingTimeframe() {
            // the state transition is scheduled based on the current event's time rather than the
            // initializationTime
            this.elapsedTime = elapsedTime.plus(COOLDOWN_TIMEOUT);

            // make sure that we're still below the scalingIntervalMax
            this.elapsedTime =
                    elapsedTime.plus(
                            RESOURCE_STABILIZATION_TIMEOUT.minus(elapsedTime).dividedBy(2));
            this.triggerOutdatedTasks();
        }

        /**
         * Transitions the context's time to a moment that falls into the test instance's
         * hard-scaling phase.
         */
        public void transitionIntoHardScalingTimeframe() {
            // the state transition is scheduled based on the current event's time rather than the
            // initializationTime
            this.elapsedTime = elapsedTime.plus(RESOURCE_STABILIZATION_TIMEOUT).plusMinutes(1);
            this.triggerOutdatedTasks();
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

        public int numberOfTasksWaiting() {
            return scheduledTasks.size();
        }

        public boolean additionalTasksWaiting() {
            return !scheduledTasks.isEmpty();
        }
    }
}
