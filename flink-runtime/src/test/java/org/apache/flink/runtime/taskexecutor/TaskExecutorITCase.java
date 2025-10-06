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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.core.testutils.CustomExtension;
import org.apache.flink.core.testutils.EachCallbackWrapper;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.AccessExecution;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.StreamGraph;
import org.apache.flink.runtime.jobgraph.StreamGraphTestUtils;
import org.apache.flink.runtime.jobgraph.StreamVertex;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmaster.StreamResult;
import org.apache.flink.runtime.jobmaster.TestingAbstractInvokables;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.InternalMiniClusterExtension;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.util.RestartStrategyUtils;
import org.apache.flink.util.function.SupplierWithException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/** Integration tests for the {@link TaskExecutor}. */
class TaskExecutorITCase {

    private static final int NUM_TMS = 2;
    private static final int SLOTS_PER_TM = 2;
    private static final int PARALLELISM = NUM_TMS * SLOTS_PER_TM;

    private static final InternalMiniClusterExtension MINI_CLUSTER_EXTENSION =
            new InternalMiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(NUM_TMS)
                            .setNumberSlotsPerTaskManager(SLOTS_PER_TM)
                            .build());

    @RegisterExtension
    private final EachCallbackWrapper<?> miniClusterExtensionWrapper =
            new EachCallbackWrapper<>(
                    new CustomExtension() {
                        @Override
                        public void before(ExtensionContext context) throws Exception {
                            MINI_CLUSTER_EXTENSION.beforeAll(context);
                        }

                        @Override
                        public void after(ExtensionContext context) throws Exception {
                            MINI_CLUSTER_EXTENSION.afterAll(context);
                        }
                    });

    /**
     * Tests that a job can be re-executed after the job has failed due to a TaskExecutor
     * termination.
     */
    @Test
    void testJobReExecutionAfterTaskExecutorTermination() throws Exception {
        final StreamGraph jobGraph = createJobGraph(PARALLELISM);

        final MiniCluster miniCluster = MINI_CLUSTER_EXTENSION.getMiniCluster();

        final CompletableFuture<StreamResult> jobResultFuture =
                submitJobAndWaitUntilRunning(jobGraph, miniCluster);

        // kill one TaskExecutor which should fail the job execution
        miniCluster.terminateTaskManager(0);

        final StreamResult jobResult = jobResultFuture.get();

        assertThat(jobResult.isSuccess()).isFalse();

        miniCluster.startTaskManager();

        final StreamGraph newJobGraph = createJobGraph(PARALLELISM);
        BlockingOperator.unblock();
        miniCluster.submitStream(newJobGraph).get();

        miniCluster.requestStreamResult(newJobGraph.getStreamID()).get();
    }

    /** Tests that the job can recover from a failing {@link TaskExecutor}. */
    @Test
    void testJobRecoveryWithFailingTaskExecutor() throws Exception {
        final StreamGraph jobGraph = createJobGraphWithRestartStrategy(PARALLELISM);

        final MiniCluster miniCluster = MINI_CLUSTER_EXTENSION.getMiniCluster();

        final CompletableFuture<StreamResult> jobResultFuture =
                submitJobAndWaitUntilRunning(jobGraph, miniCluster);

        // start an additional TaskExecutor
        miniCluster.startTaskManager();

        miniCluster.terminateTaskManager(0).get(); // this should fail the job

        BlockingOperator.unblock();

        assertThat(jobResultFuture.get().isSuccess()).isTrue();
    }

    private static CompletableFuture<StreamResult> submitJobAndWaitUntilRunning(
            StreamGraph jobGraph, MiniCluster miniCluster) throws Exception {
        miniCluster.submitStream(jobGraph).get();

        final CompletableFuture<StreamResult> jobResultFuture =
                miniCluster.requestStreamResult(jobGraph.getStreamID());

        assertThat(jobResultFuture).isNotDone();

        CommonTestUtils.waitUntilCondition(
                jobIsRunning(() -> miniCluster.getExecutionGraph(jobGraph.getStreamID())), 50L);

        return jobResultFuture;
    }

    private static SupplierWithException<Boolean, Exception> jobIsRunning(
            Supplier<CompletableFuture<? extends AccessExecutionGraph>>
                    executionGraphFutureSupplier) {
        final Predicate<AccessExecution> runningOrFinished =
                ExecutionGraphTestUtils.isInExecutionState(ExecutionState.RUNNING)
                        .or(ExecutionGraphTestUtils.isInExecutionState(ExecutionState.FINISHED));
        final Predicate<AccessExecutionGraph> allExecutionsRunning =
                ExecutionGraphTestUtils.allExecutionsPredicate(runningOrFinished);

        return () -> {
            final AccessExecutionGraph executionGraph = executionGraphFutureSupplier.get().join();
            return allExecutionsRunning.test(executionGraph)
                    && executionGraph.getState() == JobStatus.RUNNING;
        };
    }

    private StreamGraph createJobGraphWithRestartStrategy(int parallelism) throws IOException {
        final StreamGraph jobGraph = createJobGraph(parallelism);
        RestartStrategyUtils.configureFixedDelayRestartStrategy(jobGraph, 2, 0L);

        return jobGraph;
    }

    private StreamGraph createJobGraph(int parallelism) {
        final StreamVertex sender = new StreamVertex("Sender");
        sender.setParallelism(parallelism);
        sender.setInvokableClass(TestingAbstractInvokables.Sender.class);

        final StreamVertex receiver = new StreamVertex("Blocking receiver");
        receiver.setParallelism(parallelism);
        receiver.setInvokableClass(BlockingOperator.class);
        BlockingOperator.reset();

        receiver.connectNewDataSetAsInput(
                sender, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

        final SlotSharingGroup slotSharingGroup = new SlotSharingGroup();
        sender.setSlotSharingGroup(slotSharingGroup);
        receiver.setSlotSharingGroup(slotSharingGroup);

        return StreamGraphTestUtils.streamingStreamGraph(sender, receiver);
    }

    /** Blocking invokable which is controlled by a static field. */
    public static class BlockingOperator extends TestingAbstractInvokables.Receiver {
        private static CountDownLatch countDownLatch = new CountDownLatch(1);

        public BlockingOperator(Environment environment) {
            super(environment);
        }

        @Override
        public void invoke() throws Exception {
            countDownLatch.await();
            super.invoke();
        }

        public static void unblock() {
            countDownLatch.countDown();
        }

        public static void reset() {
            countDownLatch = new CountDownLatch(1);
        }
    }
}
