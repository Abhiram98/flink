/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.api.reader.RecordReader;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.api.writer.RecordWriterBuilder;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.StreamGraph;
import org.apache.flink.runtime.jobgraph.StreamVertex;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.minicluster.TestingMiniCluster;
import org.apache.flink.runtime.minicluster.TestingMiniClusterConfiguration;
import org.apache.flink.runtime.scheduler.ClusterDatasetCorruptedException;
import org.apache.flink.streaming.util.RestartStrategyUtils;
import org.apache.flink.types.IntValue;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/** Integration tests for reusing persisted intermediate dataset */
class JobIntermediateDatasetReuseTest {

    private static final Logger LOG =
            LoggerFactory.getLogger(JobIntermediateDatasetReuseTest.class);

    @Test
    void testClusterPartitionReuse() throws Exception {
        internalTestClusterPartitionReuse(
                1, 1, jobResult -> Assertions.assertThat(jobResult.isSuccess()).isTrue());
    }

    @Test
    void testClusterPartitionReuseMultipleParallelism() throws Exception {
        internalTestClusterPartitionReuse(
                64, 64, jobResult -> Assertions.assertThat(jobResult.isSuccess()).isTrue());
    }

    @Test
    void testClusterPartitionReuseWithMoreConsumerParallelismThrowException() throws Exception {
        internalTestClusterPartitionReuse(
                1,
                2,
                jobResult -> {
                    Assertions.assertThat(jobResult.isSuccess()).isFalse();
                    Assertions.assertThat(getClusterDatasetCorruptedException(jobResult))
                            .isNotNull();
                });
    }

    @Test
    void testClusterPartitionReuseWithLessConsumerParallelismThrowException() throws Exception {
        internalTestClusterPartitionReuse(
                2,
                1,
                jobResult -> {
                    Assertions.assertThat(jobResult.isSuccess()).isFalse();
                    Assertions.assertThat(getClusterDatasetCorruptedException(jobResult))
                            .isNotNull();
                });
    }

    private void internalTestClusterPartitionReuse(
            int producerParallelism,
            int consumerParallelism,
            Consumer<StreamResult> jobResultVerification)
            throws Exception {
        final TestingMiniClusterConfiguration miniClusterConfiguration =
                TestingMiniClusterConfiguration.newBuilder().build();

        try (TestingMiniCluster miniCluster =
                TestingMiniCluster.newBuilder(miniClusterConfiguration).build()) {
            miniCluster.start();

            IntermediateDataSetID intermediateDataSetID = new IntermediateDataSetID();
            final StreamGraph firstJobGraph =
                    createFirstJobGraph(producerParallelism, intermediateDataSetID);
            miniCluster.submitStream(firstJobGraph).get();
            CompletableFuture<StreamResult> jobResultFuture =
                    miniCluster.requestStreamResult(firstJobGraph.getStreamID());
            StreamResult jobResult = jobResultFuture.get();
            Assertions.assertThat(jobResult.isSuccess()).isTrue();

            final StreamGraph secondJobGraph =
                    createSecondJobGraph(consumerParallelism, intermediateDataSetID);
            miniCluster.submitStream(secondJobGraph).get();
            jobResultFuture = miniCluster.requestStreamResult(secondJobGraph.getStreamID());
            jobResult = jobResultFuture.get();
            jobResultVerification.accept(jobResult);
        }
    }

    @Test
    void testClusterPartitionReuseWithTMFail() throws Exception {
        final TestingMiniClusterConfiguration miniClusterConfiguration =
                TestingMiniClusterConfiguration.newBuilder().build();

        try (TestingMiniCluster miniCluster =
                TestingMiniCluster.newBuilder(miniClusterConfiguration).build()) {
            miniCluster.start();

            IntermediateDataSetID intermediateDataSetID = new IntermediateDataSetID();
            final StreamGraph firstJobGraph = createFirstJobGraph(1, intermediateDataSetID);
            miniCluster.submitStream(firstJobGraph).get();
            CompletableFuture<StreamResult> jobResultFuture =
                    miniCluster.requestStreamResult(firstJobGraph.getStreamID());
            StreamResult jobResult = jobResultFuture.get();
            Assertions.assertThat(jobResult.isSuccess()).isTrue();

            miniCluster.terminateTaskManager(0);
            miniCluster.startTaskManager();

            final StreamGraph secondJobGraph = createSecondJobGraph(1, intermediateDataSetID);
            RestartStrategyUtils.configureFixedDelayRestartStrategy(secondJobGraph, 1024, 1000L);
            miniCluster.submitStream(secondJobGraph).get();
            jobResultFuture = miniCluster.requestStreamResult(secondJobGraph.getStreamID());
            jobResult = jobResultFuture.get();
            Assertions.assertThat(jobResult.isSuccess()).isFalse();
            final ClusterDatasetCorruptedException exception =
                    getClusterDatasetCorruptedException(jobResult);
            Assertions.assertThat(exception).isNotNull();
            Assertions.assertThat(exception.getCorruptedClusterDatasetIds().get(0))
                    .isEqualTo(intermediateDataSetID);

            firstJobGraph.setJobID(new JobID());
            miniCluster.submitStream(firstJobGraph).get();
            jobResultFuture = miniCluster.requestStreamResult(firstJobGraph.getStreamID());
            jobResult = jobResultFuture.get();
            Assertions.assertThat(jobResult.isSuccess()).isTrue();

            secondJobGraph.setJobID(new JobID());
            miniCluster.submitStream(secondJobGraph).get();
            jobResultFuture = miniCluster.requestStreamResult(secondJobGraph.getStreamID());
            jobResult = jobResultFuture.get();
            Assertions.assertThat(jobResult.isSuccess()).isTrue();
        }
    }

    private ClusterDatasetCorruptedException getClusterDatasetCorruptedException(
            StreamResult jobResult) {
        Assertions.assertThat(jobResult.getSerializedThrowable().isPresent()).isTrue();
        Throwable throwable =
                jobResult
                        .getSerializedThrowable()
                        .get()
                        .deserializeError(Thread.currentThread().getContextClassLoader());
        while (throwable != null) {
            if (throwable instanceof ClusterDatasetCorruptedException) {
                return (ClusterDatasetCorruptedException) throwable;
            }
            throwable = throwable.getCause();
        }
        return null;
    }

    private StreamGraph createSecondJobGraph(
            int parallelism, IntermediateDataSetID intermediateDataSetID) {
        final StreamVertex receiver = new StreamVertex("Receiver 2", null);
        receiver.setParallelism(parallelism);
        receiver.setInvokableClass(Receiver.class);
        receiver.addIntermediateDataSetIdToConsume(intermediateDataSetID);

        return new StreamGraph(null, "Second Job", receiver);
    }

    private StreamGraph createFirstJobGraph(
            int parallelism, IntermediateDataSetID intermediateDataSetID) {
        final StreamVertex sender = new StreamVertex("Sender");
        sender.setParallelism(parallelism);
        sender.setInvokableClass(Sender.class);

        final StreamVertex receiver = new StreamVertex("Receiver");
        receiver.setParallelism(parallelism);
        receiver.setInvokableClass(Receiver.class);

        receiver.connectNewDataSetAsInput(
                sender,
                DistributionPattern.POINTWISE,
                ResultPartitionType.BLOCKING_PERSISTENT,
                intermediateDataSetID,
                false);

        return new StreamGraph(null, "First Job", sender, receiver);
    }

    /**
     * Basic sender {@link AbstractInvokable} which sends 100 record base on its index to down
     * stream.
     */
    public static class Sender extends AbstractInvokable {

        public Sender(Environment environment) {
            super(environment);
        }

        @Override
        public void invoke() throws Exception {
            int index = getIndexInSubtaskGroup();
            final RecordWriter<IntValue> writer =
                    new RecordWriterBuilder<IntValue>().build(getEnvironment().getWriter(0));

            try {
                for (int i = index; i < index + 100; ++i) {
                    writer.emit(new IntValue(i));
                    LOG.debug("Sender({}) emit {}", index, i);
                }
                writer.flushAll();
            } finally {
                writer.close();
            }
        }
    }

    /**
     * Basic receiver {@link AbstractInvokable} which verifies the sent elements from the {@link
     * Sender}.
     */
    public static class Receiver extends AbstractInvokable {

        public Receiver(Environment environment) {
            super(environment);
        }

        @Override
        public void invoke() throws Exception {
            int index = getIndexInSubtaskGroup();
            final RecordReader<IntValue> reader =
                    new RecordReader<>(
                            getEnvironment().getInputGate(0),
                            IntValue.class,
                            getEnvironment().getTaskManagerInfo().getTmpDirectories());
            for (int i = index; i < index + 100; ++i) {
                final int value = reader.next().getValue();
                LOG.debug("Receiver({}) received {}", index, value);
                Assertions.assertThat(value).isEqualTo(i);
            }

            Assertions.assertThat(reader.next()).isNull();
        }
    }
}
