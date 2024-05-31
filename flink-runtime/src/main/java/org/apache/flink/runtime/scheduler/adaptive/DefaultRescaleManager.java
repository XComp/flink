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
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.Temporal;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * {@code DefaultRescaleManager} manages triggering the next rescaling based on when the previous
 * rescale operation happened and the available resources. It handles the event based on the
 * following phases (in that order):
 *
 * <ol>
 *   <li>Cooldown phase: No rescaling takes place (its upper threshold is defined by {@code
 *       scalingIntervalMin}.
 *   <li>Soft-rescaling phase: Rescaling is triggered if the desired amount of resources is
 *       available.
 *   <li>Hard-rescaling phase: Rescaling is triggered if a sufficient amount of resources is
 *       available (its lower threshold is defined by (@code scalingIntervalMax}).
 * </ol>
 *
 * @see Executing
 */
public class DefaultRescaleManager implements RescaleManager {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultRescaleManager.class);

    private static final Duration MAXIMUM_DELAY_FOR_TRIGGER = Duration.ofMinutes(10);

    private final Temporal initializationTime;
    private final Supplier<Temporal> clock;

    @VisibleForTesting final Duration scalingIntervalMin;
    @VisibleForTesting @Nullable final Duration scalingIntervalMax;

    private final RescaleManager.Context rescaleContext;

    private boolean rescaleScheduled = false;

    private final Duration maxTriggerDelay;
    private CompletableFuture<Void> triggerFuture;

    DefaultRescaleManager(
            Temporal initializationTime,
            RescaleManager.Context rescaleContext,
            Duration scalingIntervalMin,
            @Nullable Duration scalingIntervalMax,
            Duration maxTriggerDelay) {
        this(
                initializationTime,
                Instant::now,
                rescaleContext,
                scalingIntervalMin,
                scalingIntervalMax,
                maxTriggerDelay);
    }

    @VisibleForTesting
    DefaultRescaleManager(
            Temporal initializationTime,
            Supplier<Temporal> clock,
            RescaleManager.Context rescaleContext,
            Duration scalingIntervalMin,
            @Nullable Duration scalingIntervalMax,
            Duration maxTriggerDelay) {
        this.initializationTime = initializationTime;
        this.clock = clock;

        this.maxTriggerDelay = maxTriggerDelay;
        this.triggerFuture = FutureUtils.completedVoidFuture();

        Preconditions.checkArgument(
                scalingIntervalMax == null || scalingIntervalMin.compareTo(scalingIntervalMax) <= 0,
                "scalingIntervalMax should at least match or be longer than scalingIntervalMin.");
        this.scalingIntervalMin = scalingIntervalMin;
        this.scalingIntervalMax = scalingIntervalMax;

        this.rescaleContext = rescaleContext;
    }

    @Override
    public void onChange() {
        rescaleContext.scheduleOperation(
                () -> {
                    if (this.triggerFuture.isDone()) {
                        this.triggerFuture =
                                scheduleOperationWithTrigger(this::evaluateChangeEvent);
                    }
                },
                Duration.ZERO);
    }

    private void evaluateChangeEvent() {
        if (timeSinceLastRescale().compareTo(scalingIntervalMin) > 0) {
            maybeRescale();
        } else if (!rescaleScheduled) {
            rescaleScheduled = true;
            rescaleContext.scheduleOperation(this::maybeRescale, scalingIntervalMin);
        }
    }

    private CompletableFuture<Void> scheduleOperationWithTrigger(Runnable callback) {
        final CompletableFuture<Void> triggerFuture = new CompletableFuture<>();
        triggerFuture.thenRun(callback);
        this.rescaleContext.scheduleOperation(
                () -> triggerFuture.complete(null), this.maxTriggerDelay);

        return triggerFuture;
    }

    @Override
    public void onTrigger() {
        rescaleContext.scheduleOperation(
                () -> {
                    if (!this.triggerFuture.isDone()) {
                        this.triggerFuture.complete(null);
                        LOG.debug(
                                "A rescale trigger event was observed causing the rescale verification logic to be initiated.");
                    } else {
                        LOG.debug(
                                "A rescale trigger event was observed outside of a rescale cycle. No action taken.");
                    }
                },
                Duration.ZERO);
    }

    private Duration timeSinceLastRescale() {
        return Duration.between(this.initializationTime, clock.get());
    }

    private void maybeRescale() {
        rescaleScheduled = false;
        if (rescaleContext.hasDesiredResources()) {
            LOG.info("Desired parallelism for job was reached: Rescaling will be triggered.");
            rescaleContext.rescale();
        } else if (scalingIntervalMax != null) {
            LOG.info(
                    "The longer the pipeline runs, the more the (small) resource gain is worth the restarting time. "
                            + "Last resource added does not meet the configured minimal parallelism change. Forced rescaling will be triggered after {} if the resource is still there.",
                    scalingIntervalMax);

            // reasoning for inconsistent scheduling:
            // https://lists.apache.org/thread/m2w2xzfjpxlw63j0k7tfxfgs0rshhwwr
            if (timeSinceLastRescale().compareTo(scalingIntervalMax) > 0) {
                rescaleWithSufficientResources();
            } else {
                rescaleContext.scheduleOperation(
                        this::rescaleWithSufficientResources, scalingIntervalMax);
            }
        }
    }

    private void rescaleWithSufficientResources() {
        if (rescaleContext.hasSufficientResources()) {
            LOG.info(
                    "Resources for desired job parallelism couldn't be collected after {}: Rescaling will be enforced.",
                    scalingIntervalMax);
            rescaleContext.rescale();
        }
    }

    public static class Factory implements RescaleManager.Factory {

        private final Duration scalingIntervalMin;
        @Nullable private final Duration scalingIntervalMax;

        /**
         * Creates a {@code Factory} instance based on the {@link AdaptiveScheduler}'s {@code
         * Settings} for rescaling.
         */
        public static Factory fromSettings(AdaptiveScheduler.Settings settings) {
            // it's not ideal that we use a AdaptiveScheduler internal class here. We might want to
            // change that as part of a more general alignment of the rescaling configuration.
            return new Factory(settings.getScalingIntervalMin(), settings.getScalingIntervalMax());
        }

        private Factory(Duration scalingIntervalMin, @Nullable Duration scalingIntervalMax) {
            this.scalingIntervalMin = scalingIntervalMin;
            this.scalingIntervalMax = scalingIntervalMax;
        }

        @Override
        public DefaultRescaleManager create(Context rescaleContext, Instant lastRescale) {
            return new DefaultRescaleManager(
                    lastRescale,
                    rescaleContext,
                    scalingIntervalMin,
                    scalingIntervalMax,
                    MAXIMUM_DELAY_FOR_TRIGGER);
        }
    }
}
