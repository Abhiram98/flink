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
import org.apache.flink.util.ConfigurationException;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DefaultStateTransitionManagerTest {

    @Test
    void testProperConfiguration() throws ConfigurationException {
        final Duration scalingIntervalMin = Duration.ofMillis(1337);
        final Duration scalingIntervalMax = Duration.ofMillis(7331);
        final Duration maximumDelayForRescaleTrigger = Duration.ofMillis(4242);

        final Configuration configuration = new Configuration();
        configuration.set(JobManagerOptions.SCHEDULER_SCALING_INTERVAL_MIN, scalingIntervalMin);
        configuration.set(JobManagerOptions.SCHEDULER_SCALING_INTERVAL_MAX, scalingIntervalMax);
        configuration.set(
                JobManagerOptions.MAXIMUM_DELAY_FOR_SCALE_TRIGGER, maximumDelayForRescaleTrigger);

        final DefaultStateTransitionManager testInstance =
                DefaultStateTransitionManager.Factory.fromSettings(
                                AdaptiveScheduler.Settings.of(configuration))
                        .create(TestingStateTransitionManagerContext.stableContext(), Instant.now());
        assertThat(testInstance.scalingIntervalMin).isEqualTo(scalingIntervalMin);
        assertThat(testInstance.scalingIntervalMax).isEqualTo(scalingIntervalMax);
        assertThat(testInstance.maxTriggerDelay).isEqualTo(maximumDelayForRescaleTrigger);
    }

    @Test
    void testInvalidConfiguration() {
        final Duration cooldownThreshold = Duration.ofMinutes(2);
        final TestingStateTransitionManagerContext ctx = TestingStateTransitionManagerContext.stableContext();
        assertThatThrownBy(
                        () ->
                                new DefaultStateTransitionManager(
                                        Instant.now(),
                                        ctx,
                                        cooldownThreshold,
                                        cooldownThreshold.minusNanos(1),
                                        Duration.ofHours(5)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void triggerWithoutChangeEventNoopInCooldownPhase() {
        triggerWithoutChangeEventNoop(
                TestingStateTransitionManagerContext::createTestInstanceInCooldownPhase);
    }

    @Test
    void triggerWithoutChangeEventNoopInSoftRescalingPhase() {
        triggerWithoutChangeEventNoop(
                TestingStateTransitionManagerContext::createTestInstanceInSoftRescalePhase);
    }

    @Test
    void triggerWithoutChangeEventNoopInHardRescalingPhase() {
        triggerWithoutChangeEventNoop(
                TestingStateTransitionManagerContext::createTestInstanceInHardRescalePhase);
    }

    private void triggerWithoutChangeEventNoop(
            Function<TestingStateTransitionManagerContext, DefaultStateTransitionManager> testInstanceCreator) {
        final TestingStateTransitionManagerContext ctx =
                TestingStateTransitionManagerContext.stableContext().withDesiredRescaling();
        final DefaultStateTransitionManager testInstance = testInstanceCreator.apply(ctx);

        testInstance.onTrigger();

        assertThat(ctx.rescaleWasTriggered())
                .as(
                        "No rescaling should have been triggered due to the missing change event despite the fact that desired rescaling would be possible.")
                .isFalse();
        assertThat(ctx.additionalTasksWaiting()).as("No tasks should be scheduled.").isFalse();
    }

    @Test
    void testDesiredChangeEventDuringCooldown() {
        final TestingStateTransitionManagerContext softScalePossibleCtx =
                TestingStateTransitionManagerContext.stableContext().withDesiredRescaling();
        final DefaultStateTransitionManager testInstance =
                softScalePossibleCtx.createTestInstanceInCooldownPhase();

        testInstance.onChange();

        assertIntermediateStateWithoutRescale(softScalePossibleCtx);

        testInstance.onTrigger();

        assertIntermediateStateWithoutRescale(softScalePossibleCtx);

        softScalePossibleCtx.transitionIntoSoftScalingTimeframe();

        assertFinalStateWithRescale(softScalePossibleCtx);
    }

    @Test
    void testDesiredChangeEventInSoftRescalePhase() {
        final TestingStateTransitionManagerContext desiredRescalePossibleCtx =
                TestingStateTransitionManagerContext.stableContext().withDesiredRescaling();
        final DefaultStateTransitionManager testInstance =
                desiredRescalePossibleCtx.createTestInstanceInSoftRescalePhase();

        testInstance.onChange();

        assertIntermediateStateWithoutRescale(desiredRescalePossibleCtx);

        testInstance.onTrigger();

        assertFinalStateWithRescale(desiredRescalePossibleCtx);
    }

    @Test
    void testDesiredChangeEventInHardRescalePhase() {
        final TestingStateTransitionManagerContext desiredRescalePossibleCtx =
                TestingStateTransitionManagerContext.stableContext().withDesiredRescaling();
        final DefaultStateTransitionManager testInstance =
                desiredRescalePossibleCtx.createTestInstanceInHardRescalePhase();

        testInstance.onChange();

        assertIntermediateStateWithoutRescale(desiredRescalePossibleCtx);

        testInstance.onTrigger();

        assertFinalStateWithRescale(desiredRescalePossibleCtx);
    }

    @Test
    void testNoRescaleInCooldownPhase() {
        final TestingStateTransitionManagerContext noRescalePossibleCtx =
                TestingStateTransitionManagerContext.stableContext();
        final DefaultStateTransitionManager testInstance =
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
        final TestingStateTransitionManagerContext noRescalePossibleCtx =
                TestingStateTransitionManagerContext.stableContext();
        final DefaultStateTransitionManager testInstance =
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
        final TestingStateTransitionManagerContext noRescalePossibleCtx =
                TestingStateTransitionManagerContext.stableContext();
        final DefaultStateTransitionManager testInstance =
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
        final TestingStateTransitionManagerContext hardRescalePossibleCtx =
                TestingStateTransitionManagerContext.stableContext().withSufficientRescaling();
        final DefaultStateTransitionManager testInstance =
                hardRescalePossibleCtx.createTestInstanceInCooldownPhase();

        testInstance.onChange();

        assertIntermediateStateWithoutRescale(hardRescalePossibleCtx);

        testInstance.onTrigger();

        assertIntermediateStateWithoutRescale(hardRescalePossibleCtx);

        hardRescalePossibleCtx.transitionIntoSoftScalingTimeframe();

        assertIntermediateStateWithoutRescale(hardRescalePossibleCtx);

        hardRescalePossibleCtx.transitionIntoHardScalingTimeframe();

        assertFinalStateWithRescale(hardRescalePossibleCtx);
    }

    @Test
    void testSufficientChangeInSoftRescalePhase() {
        final TestingStateTransitionManagerContext hardRescalePossibleCtx =
                TestingStateTransitionManagerContext.stableContext().withSufficientRescaling();
        final DefaultStateTransitionManager testInstance =
                hardRescalePossibleCtx.createTestInstanceInSoftRescalePhase();

        testInstance.onChange();

        assertIntermediateStateWithoutRescale(hardRescalePossibleCtx);

        testInstance.onTrigger();

        assertIntermediateStateWithoutRescale(hardRescalePossibleCtx);

        hardRescalePossibleCtx.transitionIntoHardScalingTimeframe();

        assertFinalStateWithRescale(hardRescalePossibleCtx);
    }

    @Test
    void testSufficientChangeInHardRescalePhase() {
        final TestingStateTransitionManagerContext hardRescalePossibleCtx =
                TestingStateTransitionManagerContext.stableContext().withSufficientRescaling();
        final DefaultStateTransitionManager testInstance =
                hardRescalePossibleCtx.createTestInstanceInHardRescalePhase();

        testInstance.onChange();

        assertIntermediateStateWithoutRescale(hardRescalePossibleCtx);

        testInstance.onTrigger();

        assertFinalStateWithRescale(hardRescalePossibleCtx);
    }

    @Test
    void testSufficientChangeInCooldownWithSubsequentDesiredChangeInSoftRescalePhase() {
        final TestingStateTransitionManagerContext ctx =
                TestingStateTransitionManagerContext.stableContext().withSufficientRescaling();
        final DefaultStateTransitionManager testInstance = ctx.createTestInstanceInCooldownPhase();

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
                .as(
                        "There should be a task scheduled that allows transitioning into hard-rescaling phase.")
                .isEqualTo(3);
    }

    @Test
    void testSufficientChangeWithSubsequentDesiredChangeInSoftRescalePhase() {
        final TestingStateTransitionManagerContext ctx =
                TestingStateTransitionManagerContext.stableContext().withSufficientRescaling();
        final DefaultStateTransitionManager testInstance = ctx.createTestInstanceInSoftRescalePhase();

        testInstance.onChange();

        assertIntermediateStateWithoutRescale(ctx);

        testInstance.onTrigger();

        assertIntermediateStateWithoutRescale(ctx);

        assertThat(ctx.numberOfTasksWaiting())
                .as(
                        "There should be a task scheduled that allows transitioning into hard-rescaling phase.")
                .isEqualTo(2);

        ctx.withDesiredRescaling();

        testInstance.onChange();

        assertIntermediateStateWithoutRescale(ctx);

        testInstance.onTrigger();

        assertThat(ctx.rescaleWasTriggered()).isTrue();
    }

    @Test
    void
            testRevokedSufficientChangeInSoftRescalePhaseWithSubsequentSufficientChangeInHardRescalingPhase() {
        final TestingStateTransitionManagerContext ctx =
                TestingStateTransitionManagerContext.stableContext().withSufficientRescaling();
        final DefaultStateTransitionManager testInstance = ctx.createTestInstanceInSoftRescalePhase();

        testInstance.onChange();

        assertIntermediateStateWithoutRescale(ctx);

        testInstance.onTrigger();

        assertIntermediateStateWithoutRescale(ctx);

        assertThat(ctx.numberOfTasksWaiting())
                .as(
                        "There should be a task scheduled that allows transitioning into hard-rescaling phase.")
                .isEqualTo(2);

        ctx.revertAnyParallelismImprovements();

        testInstance.onChange();

        assertIntermediateStateWithoutRescale(ctx);

        testInstance.onTrigger();

        assertIntermediateStateWithoutRescale(ctx);

        assertThat(ctx.numberOfTasksWaiting())
                .as(
                        "There should be a task scheduled that allows transitioning into hard-rescaling phase.")
                .isEqualTo(2);

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
    void testRevokedChangeInHardRescalingPhaseCausesWithSubsequentSufficientChange() {
        final TestingStateTransitionManagerContext ctx = TestingStateTransitionManagerContext.stableContext();
        final DefaultStateTransitionManager testInstance = ctx.createTestInstanceInHardRescalePhase();

        testInstance.onChange();

        assertIntermediateStateWithoutRescale(ctx);

        testInstance.onTrigger();

        assertThat(ctx.rescaleWasTriggered()).isFalse();
        assertThat(ctx.additionalTasksWaiting()).isTrue();

        ctx.withSufficientRescaling();

        testInstance.onChange();

        assertIntermediateStateWithoutRescale(ctx);

        testInstance.onTrigger();

        assertFinalStateWithRescale(ctx);
    }

    private static void assertIntermediateStateWithoutRescale(TestingStateTransitionManagerContext ctx) {
        assertThat(ctx.rescaleWasTriggered())
                .as("The rescale should not have been triggered, yet.")
                .isFalse();
        assertThat(ctx.additionalTasksWaiting())
                .as("There should be still tasks being scheduled.")
                .isTrue();
    }

    private static void assertFinalStateWithRescale(TestingStateTransitionManagerContext ctx) {
        assertThat(ctx.rescaleWasTriggered())
                .as("The rescale should have been triggered already.")
                .isTrue();
        assertThat(ctx.additionalTasksWaiting())
                .as("All scheduled tasks should have been executed.")
                .isTrue();
    }

    /**
     * {@code TestingStateTransitionManagerContext} provides methods for adjusting the elapsed time and for
     * adjusting the available resources for rescaling.
     */
    private static class TestingStateTransitionManagerContext implements StateTransitionManager.Context {

        // default configuration values to allow for easy transitioning between the phases
        private static final Duration SCALING_MIN = Duration.ofHours(1);
        private static final Duration SCALING_MAX = Duration.ofHours(2);

        // configuration that defines what kind of rescaling would be possible
        private boolean hasSufficientResources = false;
        private boolean hasDesiredResources = false;

        // internal state used for assertions
        private final AtomicBoolean rescaleTriggered = new AtomicBoolean();
        private final SortedMap<Instant, List<Runnable>> scheduledTasks = new TreeMap<>();

        // Instant.MIN makes debugging easier because timestamps become human-readable
        private final Instant initializationTime = Instant.MIN;
        private Duration elapsedTime = Duration.ZERO;

        // ///////////////////////////////////////////////
        // Context creation
        // ///////////////////////////////////////////////

        public static TestingStateTransitionManagerContext stableContext() {
            return new TestingStateTransitionManagerContext();
        }

        private TestingStateTransitionManagerContext() {
            // no rescaling is enabled by default
            revertAnyParallelismImprovements();
        }

        public void revertAnyParallelismImprovements() {
            this.hasSufficientResources = false;
            this.hasDesiredResources = false;
        }

        public TestingStateTransitionManagerContext withDesiredRescaling() {
            // having desired resources should also mean that the sufficient resources are met
            this.hasSufficientResources = true;
            this.hasDesiredResources = true;

            return this;
        }

        public TestingStateTransitionManagerContext withSufficientRescaling() {
            this.hasSufficientResources = true;
            this.hasDesiredResources = false;

            return this;
        }

        // ///////////////////////////////////////////////
        // StateTransitionManager.Context interface methods
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
         * Creates the {@code DefaultStateTransitionManager} test instance and transitions into a period in
         * time where the instance is in cooldown phase.
         */
        public DefaultStateTransitionManager createTestInstanceInCooldownPhase() {
            return createTestInstance(this::transitionIntoCooldownTimeframe);
        }

        /**
         * Creates the {@code DefaultStateTransitionManager} test instance and transitions into a period in
         * time where the instance is in soft-rescaling phase.
         */
        public DefaultStateTransitionManager createTestInstanceInSoftRescalePhase() {
            return createTestInstance(this::transitionIntoSoftScalingTimeframe);
        }

        /**
         * Creates the {@code DefaultStateTransitionManager} test instance and transitions into a period in
         * time where the instance is in hard-rescaling phase.
         */
        public DefaultStateTransitionManager createTestInstanceInHardRescalePhase() {
            return createTestInstance(this::transitionIntoHardScalingTimeframe);
        }

        /**
         * Initializes the test instance and sets the context's elapsed time based on the passed
         * callback.
         */
        private DefaultStateTransitionManager createTestInstance(Runnable timeTransitioning) {
            final DefaultStateTransitionManager testInstance =
                    new DefaultStateTransitionManager(
                            initializationTime,
                            // clock that returns the time based on the configured elapsedTime
                            () -> Objects.requireNonNull(initializationTime).plus(elapsedTime),
                            this,
                            SCALING_MIN,
                            SCALING_MAX,
                            Duration.ofHours(5)) {
                        @Override
                        public void onChange() {
                            super.onChange();

                            // hack to avoid calling this method in every test method
                            // we want to trigger tasks that are meant to run right-away
                            TestingStateTransitionManagerContext.this.triggerOutdatedTasks();
                        }

                        @Override
                        public void onTrigger() {
                            super.onTrigger();

                            // hack to avoid calling this method in every test method
                            // we want to trigger tasks that are meant to run right-away
                            TestingStateTransitionManagerContext.this.triggerOutdatedTasks();
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
            this.elapsedTime = SCALING_MIN.dividedBy(2);
            this.triggerOutdatedTasks();
        }

        /**
         * Transitions the context's time to a moment that falls into the test instance's
         * soft-scaling phase.
         */
        public void transitionIntoSoftScalingTimeframe() {
            // the state transition is scheduled based on the current event's time rather than the
            // initializationTime
            this.elapsedTime = elapsedTime.plus(SCALING_MIN);

            // make sure that we're still below the scalingIntervalMax
            this.elapsedTime = elapsedTime.plus(SCALING_MAX.minus(elapsedTime).dividedBy(2));
            this.triggerOutdatedTasks();
        }

        /**
         * Transitions the context's time to a moment that falls into the test instance's
         * hard-scaling phase.
         */
        public void transitionIntoHardScalingTimeframe() {
            // the state transition is scheduled based on the current event's time rather than the
            // initializationTime
            this.elapsedTime = elapsedTime.plus(SCALING_MAX).plusMinutes(1);
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
