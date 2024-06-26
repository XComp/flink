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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.Temporal;
import java.util.function.Supplier;

/**
 * {@code DefaultRescaleManager} is a state machine which manages the rescaling based on the
 * previous rescale operation and the available resources. See {@link State} for details on each
 * individual state.
 *
 * <pre>
 * {@link Cooldown}
 *   |
 *   +--> {@link Idling}
 *   |      |
 *   |      V
 *   +--> {@link Stabilizing}
 *          |
 *          +--> {@link Stabilized} --> {@link Idling}
 *          |      |
 *          |      V
 *          \--> Rescaling
 * </pre>
 *
 * <p>Thread-safety: This class is not implemented in a thread-safe manner and relies on the fact
 * that any method call happens within a single thread.
 *
 * @see Executing
 */
@NotThreadSafe
public class DefaultRescaleManager implements RescaleManager {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultRescaleManager.class);

    private final Supplier<Temporal> clock;

    @VisibleForTesting final Duration cooldownTimeout;
    @VisibleForTesting final Duration resourceStabilizationTimeout;

    private final RescaleManager.Context rescaleContext;

    @VisibleForTesting final Duration maxTriggerDelay;

    @Nullable private State state;

    DefaultRescaleManager(
            Temporal initializationTime,
            RescaleManager.Context rescaleContext,
            Duration cooldownTimeout,
            Duration resourceStabilizationTimeout,
            Duration maxTriggerDelay) {
        this(
                initializationTime,
                Instant::now,
                rescaleContext,
                cooldownTimeout,
                resourceStabilizationTimeout,
                maxTriggerDelay);
    }

    @VisibleForTesting
    DefaultRescaleManager(
            Temporal initializationTime,
            Supplier<Temporal> clock,
            RescaleManager.Context rescaleContext,
            Duration cooldownTimeout,
            Duration resourceStabilizationTimeout,
            Duration maxTriggerDelay) {
        this.clock = clock;

        this.maxTriggerDelay = maxTriggerDelay;

        this.cooldownTimeout = cooldownTimeout;
        this.resourceStabilizationTimeout = resourceStabilizationTimeout;

        this.rescaleContext = rescaleContext;

        this.state = new Cooldown(initializationTime, clock, this, cooldownTimeout);
    }

    @Override
    public void onChange() {
        state.onChange();
    }

    @Override
    public void onTrigger() {
        state.onTrigger();
    }

    private void transitionToIdle() {
        // TODO: resourceWaitTimeout is not available, yet
        transitionToState(new Idling(clock, this, null));
    }

    @Nullable
    @VisibleForTesting
    State getState() {
        return state;
    }

    private void transitionToStabilizing(Temporal firstChangeEventTimestamp) {
        transitionToState(
                new Stabilizing(
                        clock, this, this.resourceStabilizationTimeout, firstChangeEventTimestamp));
    }

    private void transitionToStabilized(Temporal firstChangeEventTimestamp) {
        transitionToState(new Stabilized(clock, this, firstChangeEventTimestamp, maxTriggerDelay));
    }

    private void transitionToState(State newState) {
        Preconditions.checkState(
                this.state != null, "The rescaling operation was already triggered.");
        LOG.debug("Transitioning from {} to {}.", this.state, newState);
        this.state = newState;
    }

    private void transitionToRescale() {
        this.rescaleContext.rescale();
        this.state = null;
    }

    @VisibleForTesting
    abstract static class State {

        private final Supplier<Temporal> clock;
        private final DefaultRescaleManager context;

        State(Supplier<Temporal> clock, DefaultRescaleManager context) {
            this.clock = clock;
            this.context = context;
        }

        protected Temporal now() {
            return this.clock.get();
        }

        protected void scheduleRelativelyTo(
                Runnable callback, Temporal startOfTimeout, Duration timeout) {
            final Duration timeoutLeft =
                    timeout.minus(Duration.between(startOfTimeout, clock.get()));
            scheduleFromNow(callback, timeoutLeft.isNegative() ? Duration.ZERO : timeoutLeft);
        }

        protected void scheduleFromNow(Runnable callback, Duration delay) {
            context.rescaleContext.scheduleOperation(callback, delay);
        }

        protected DefaultRescaleManager context() {
            return context;
        }

        public void onChange() {}

        public void onTrigger() {}
    }

    /**
     * {@link State} to prevent any rescaling. {@link RescaleManager#onChange()} events will be
     * monitored and forwarded to the next state. {@link RescaleManager#onTrigger()} events will be
     * ignored.
     */
    @VisibleForTesting
    static class Cooldown extends State {

        @Nullable private Temporal firstChangeEventTimestamp;

        Cooldown(
                Temporal timeOfLastRescale,
                Supplier<Temporal> clock,
                DefaultRescaleManager context,
                Duration cooldownTimeout) {
            super(clock, context);

            this.scheduleRelativelyTo(this::finalizeCooldown, timeOfLastRescale, cooldownTimeout);
        }

        private void finalizeCooldown() {
            if (firstChangeEventTimestamp == null) {
                context().transitionToIdle();
            } else {
                context().transitionToStabilizing(firstChangeEventTimestamp);
            }
        }

        @Override
        public void onChange() {
            if (firstChangeEventTimestamp == null) {
                firstChangeEventTimestamp = this.now();
            }
        }
    }

    /**
     * {@link State} which succeeds the {@link Cooldown} state if no {@link
     * RescaleManager#onChange()} was observed, yet. The {@code DefaultRescaleManager} waits for a
     * first {@link RescaleManager#onChange()} event. {@link RescaleManager#onTrigger()} events will
     * be ignored.
     */
    @VisibleForTesting
    static class Idling extends State {

        Idling(
                Supplier<Temporal> clock,
                DefaultRescaleManager context,
                @Nullable Duration resourceWaitTimeout) {
            super(clock, context);

            if (resourceWaitTimeout != null) {
                scheduleFromNow(context::transitionToRescale, resourceWaitTimeout);
            }
        }

        @Override
        public void onChange() {
            context().transitionToStabilizing(now());
        }
    }

    /**
     * {@link State} that handles the resources stabilization. In this state, {@link
     * RescaleManager#onTrigger()} will initiate rescaling iff desired resources are met.
     */
    @VisibleForTesting
    static class Stabilizing extends State {

        Stabilizing(
                Supplier<Temporal> clock,
                DefaultRescaleManager context,
                Duration resourceStabilizationTimeout,
                Temporal firstOnChangeEventTimestamp) {
            super(clock, context);

            this.scheduleRelativelyTo(
                    () -> context().transitionToStabilized(firstOnChangeEventTimestamp),
                    firstOnChangeEventTimestamp,
                    resourceStabilizationTimeout);
        }

        @Override
        public void onTrigger() {
            if (this.context().rescaleContext.hasDesiredResources()) {
                context().transitionToRescale();
            }
        }
    }

    /**
     * {@link State} that handles the post-stabilization phase. A {@link RescaleManager#onTrigger()}
     * event initiates rescaling iff sufficient resources are available; otherwise transitioning to
     * {@link Idling} will be performed.
     */
    @VisibleForTesting
    static class Stabilized extends State {

        Stabilized(
                Supplier<Temporal> clock,
                DefaultRescaleManager context,
                Temporal firstChangeEventTimestamp,
                Duration maxTriggerDelay) {
            super(clock, context);

            this.scheduleRelativelyTo(this::onTrigger, firstChangeEventTimestamp, maxTriggerDelay);
        }

        @Override
        public void onTrigger() {
            if (context().rescaleContext.hasSufficientResources()) {
                context().transitionToRescale();
            } else {
                context().transitionToIdle();
            }
        }
    }

    public static class Factory implements RescaleManager.Factory {

        private final Duration cooldownTimeout;
        private final Duration resourceStabilizationTimeout;
        private final Duration maximumDelayForTrigger;

        /**
         * Creates a {@code Factory} instance based on the {@link AdaptiveScheduler}'s {@code
         * Settings} for rescaling.
         */
        public static Factory fromSettings(AdaptiveScheduler.Settings settings) {
            // it's not ideal that we use a AdaptiveScheduler internal class here. We might want to
            // change that as part of a more general alignment of the rescaling configuration.
            return new Factory(
                    settings.getScalingIntervalMin(),
                    settings.getResourceStabilizationTimeout(),
                    settings.getMaximumDelayForTriggeringRescale());
        }

        private Factory(
                Duration cooldownTimeout,
                @Nullable Duration resourceStabilizationTimeout,
                Duration maximumDelayForTrigger) {
            this.cooldownTimeout = cooldownTimeout;
            this.resourceStabilizationTimeout = resourceStabilizationTimeout;
            this.maximumDelayForTrigger = maximumDelayForTrigger;
        }

        @Override
        public DefaultRescaleManager create(Context rescaleContext, Instant lastRescale) {
            return new DefaultRescaleManager(
                    lastRescale,
                    rescaleContext,
                    cooldownTimeout,
                    resourceStabilizationTimeout,
                    maximumDelayForTrigger);
        }
    }
}
