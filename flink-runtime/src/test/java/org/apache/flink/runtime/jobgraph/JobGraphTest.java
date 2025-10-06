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

package org.apache.flink.runtime.jobgraph;

import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroup;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.configuration.ConfigurationUtils.getDoubleConfigOption;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests for {@link StreamGraph}. */
public class JobGraphTest extends TestLogger {

    @Test
    public void testSerialization() {
        try {
            StreamGraph jg = new StreamGraph("The graph");

            // add some configuration values
            {
                jg.getJobConfiguration().setString("some key", "some value");
                jg.getJobConfiguration().set(getDoubleConfigOption("Life of "), Math.PI);
            }

            // add some vertices
            {
                StreamVertex source1 = new StreamVertex("source1");
                StreamVertex source2 = new StreamVertex("source2");
                StreamVertex target = new StreamVertex("target");
                target.connectNewDataSetAsInput(
                        source1, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
                target.connectNewDataSetAsInput(
                        source2, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

                jg.addVertex(source1);
                jg.addVertex(source2);
                jg.addVertex(target);
            }

            // de-/serialize and compare
            StreamGraph copy = CommonTestUtils.createCopySerializable(jg);

            assertEquals(jg.getName(), copy.getName());
            assertEquals(jg.getStreamID(), copy.getStreamID());
            assertEquals(jg.getJobConfiguration(), copy.getJobConfiguration());
            assertEquals(jg.getNumberOfVertices(), copy.getNumberOfVertices());

            for (StreamVertex vertex : copy.getVertices()) {
                StreamVertex original = jg.findVertexByID(vertex.getID());
                assertNotNull(original);
                assertEquals(original.getName(), vertex.getName());
                assertEquals(original.getNumberOfInputs(), vertex.getNumberOfInputs());
                assertEquals(
                        original.getNumberOfProducedIntermediateDataSets(),
                        vertex.getNumberOfProducedIntermediateDataSets());
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testTopologicalSort1() {
        StreamVertex source1 = new StreamVertex("source1");
        StreamVertex source2 = new StreamVertex("source2");
        StreamVertex target1 = new StreamVertex("target1");
        StreamVertex target2 = new StreamVertex("target2");
        StreamVertex intermediate1 = new StreamVertex("intermediate1");
        StreamVertex intermediate2 = new StreamVertex("intermediate2");

        target1.connectNewDataSetAsInput(
                source1, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
        target2.connectNewDataSetAsInput(
                source1, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
        target2.connectNewDataSetAsInput(
                intermediate2, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
        intermediate2.connectNewDataSetAsInput(
                intermediate1, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
        intermediate1.connectNewDataSetAsInput(
                source2, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

        StreamGraph graph =
                StreamGraphTestUtils.streamingStreamGraph(
                        source1, source2, intermediate1, intermediate2, target1, target2);

        List<StreamVertex> sorted = graph.getVerticesSortedTopologicallyFromSources();

        assertEquals(6, sorted.size());

        assertBefore(source1, target1, sorted);
        assertBefore(source1, target2, sorted);
        assertBefore(source2, target2, sorted);
        assertBefore(source2, intermediate1, sorted);
        assertBefore(source2, intermediate2, sorted);
        assertBefore(intermediate1, target2, sorted);
        assertBefore(intermediate2, target2, sorted);
    }

    @Test
    public void testTopologicalSort2() {
        try {
            StreamVertex source1 = new StreamVertex("source1");
            StreamVertex source2 = new StreamVertex("source2");
            StreamVertex root = new StreamVertex("root");
            StreamVertex l11 = new StreamVertex("layer 1 - 1");
            StreamVertex l12 = new StreamVertex("layer 1 - 2");
            StreamVertex l13 = new StreamVertex("layer 1 - 3");
            StreamVertex l2 = new StreamVertex("layer 2");

            root.connectNewDataSetAsInput(
                    l13, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
            root.connectNewDataSetAsInput(
                    source2, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
            root.connectNewDataSetAsInput(
                    l2, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

            l2.connectNewDataSetAsInput(
                    l11, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
            l2.connectNewDataSetAsInput(
                    l12, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

            l11.connectNewDataSetAsInput(
                    source1, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

            l12.connectNewDataSetAsInput(
                    source1, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
            l12.connectNewDataSetAsInput(
                    source2, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

            l13.connectNewDataSetAsInput(
                    source2, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

            StreamGraph graph =
                    StreamGraphTestUtils.streamingStreamGraph(source1, source2, root, l11, l13, l12, l2);
            List<StreamVertex> sorted = graph.getVerticesSortedTopologicallyFromSources();

            assertEquals(7, sorted.size());

            assertBefore(source1, root, sorted);
            assertBefore(source2, root, sorted);
            assertBefore(l11, root, sorted);
            assertBefore(l12, root, sorted);
            assertBefore(l13, root, sorted);
            assertBefore(l2, root, sorted);

            assertBefore(l11, l2, sorted);
            assertBefore(l12, l2, sorted);
            assertBefore(l2, root, sorted);

            assertBefore(source1, l2, sorted);
            assertBefore(source2, l2, sorted);

            assertBefore(source2, l13, sorted);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testTopologicalSort3() {
        //             --> op1 --
        //            /         \
        //  (source) -           +-> op2 -> op3
        //            \         /
        //             ---------

        try {
            StreamVertex source = new StreamVertex("source");
            StreamVertex op1 = new StreamVertex("op4");
            StreamVertex op2 = new StreamVertex("op2");
            StreamVertex op3 = new StreamVertex("op3");

            op1.connectNewDataSetAsInput(
                    source, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
            op2.connectNewDataSetAsInput(
                    op1, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
            op2.connectNewDataSetAsInput(
                    source, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
            op3.connectNewDataSetAsInput(
                    op2, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

            StreamGraph graph = StreamGraphTestUtils.streamingStreamGraph(source, op1, op2, op3);
            List<StreamVertex> sorted = graph.getVerticesSortedTopologicallyFromSources();

            assertEquals(4, sorted.size());

            assertBefore(source, op1, sorted);
            assertBefore(source, op2, sorted);
            assertBefore(op1, op2, sorted);
            assertBefore(op2, op3, sorted);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testTopoSortCyclicGraphNoSources() {
        try {
            StreamVertex v1 = new StreamVertex("1");
            StreamVertex v2 = new StreamVertex("2");
            StreamVertex v3 = new StreamVertex("3");
            StreamVertex v4 = new StreamVertex("4");

            v1.connectNewDataSetAsInput(
                    v4, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
            v2.connectNewDataSetAsInput(
                    v1, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
            v3.connectNewDataSetAsInput(
                    v2, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
            v4.connectNewDataSetAsInput(
                    v3, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

            StreamGraph jg = StreamGraphTestUtils.streamingStreamGraph(v1, v2, v3, v4);
            try {
                jg.getVerticesSortedTopologicallyFromSources();
                fail("Failed to raise error on topologically sorting cyclic graph.");
            } catch (InvalidProgramException e) {
                // that what we wanted
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testTopoSortCyclicGraphIntermediateCycle() {
        try {
            StreamVertex source = new StreamVertex("source");
            StreamVertex v1 = new StreamVertex("1");
            StreamVertex v2 = new StreamVertex("2");
            StreamVertex v3 = new StreamVertex("3");
            StreamVertex v4 = new StreamVertex("4");
            StreamVertex target = new StreamVertex("target");

            v1.connectNewDataSetAsInput(
                    source, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
            v1.connectNewDataSetAsInput(
                    v4, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
            v2.connectNewDataSetAsInput(
                    v1, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
            v3.connectNewDataSetAsInput(
                    v2, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
            v4.connectNewDataSetAsInput(
                    v3, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
            target.connectNewDataSetAsInput(
                    v3, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

            StreamGraph jg = StreamGraphTestUtils.streamingStreamGraph(v1, v2, v3, v4, source, target);
            try {
                jg.getVerticesSortedTopologicallyFromSources();
                fail("Failed to raise error on topologically sorting cyclic graph.");
            } catch (InvalidProgramException e) {
                // that what we wanted
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    private static final void assertBefore(StreamVertex v1, StreamVertex v2, List<StreamVertex> list) {
        boolean seenFirst = false;
        for (StreamVertex v : list) {
            if (v == v1) {
                seenFirst = true;
            } else if (v == v2) {
                if (!seenFirst) {
                    fail(
                            "The first vertex ("
                                    + v1
                                    + ") is not before the second vertex ("
                                    + v2
                                    + ")");
                }
                break;
            }
        }
    }

    @Test
    public void testSetUserArtifactBlobKey() throws IOException, ClassNotFoundException {
        StreamGraph jb = StreamGraphTestUtils.emptyJobGraph();

        final DistributedCache.DistributedCacheEntry[] entries = {
            new DistributedCache.DistributedCacheEntry("p1", true, true),
            new DistributedCache.DistributedCacheEntry("p2", true, false),
            new DistributedCache.DistributedCacheEntry("p3", false, true),
            new DistributedCache.DistributedCacheEntry("p4", true, false),
        };

        for (DistributedCache.DistributedCacheEntry entry : entries) {
            jb.addUserArtifact(entry.filePath, entry);
        }

        for (DistributedCache.DistributedCacheEntry entry : entries) {
            PermanentBlobKey blobKey = new PermanentBlobKey();
            jb.setUserArtifactBlobKey(entry.filePath, blobKey);

            DistributedCache.DistributedCacheEntry jobGraphEntry =
                    jb.getUserArtifacts().get(entry.filePath);
            assertNotNull(jobGraphEntry);
            assertEquals(
                    blobKey,
                    InstantiationUtil.deserializeObject(
                            jobGraphEntry.blobKey, ClassLoader.getSystemClassLoader()));
            assertEquals(entry.isExecutable, jobGraphEntry.isExecutable);
            assertEquals(entry.isZipped, jobGraphEntry.isZipped);
            assertEquals(entry.filePath, jobGraphEntry.filePath);
        }
    }

    @Test
    public void checkpointingIsDisabledByDefaultForStreamingJobGraph() {
        final StreamGraph jobGraph = JobGraphBuilder.newStreamingJobGraphBuilder().build();

        assertFalse(jobGraph.isCheckpointingEnabled());
    }

    @Test
    public void checkpointingIsDisabledByDefaultForBatchJobGraph() {
        final StreamGraph jobGraph = JobGraphBuilder.newBatchJobGraphBuilder().build();

        assertFalse(jobGraph.isCheckpointingEnabled());
    }

    @Test
    public void checkpointingIsEnabledIfIntervalIsqAndLegal() {
        final StreamGraph jobGraph =
                JobGraphBuilder.newStreamingJobGraphBuilder()
                        .setJobCheckpointingSettings(createCheckpointSettingsWithInterval(10))
                        .build();

        assertTrue(jobGraph.isCheckpointingEnabled());
    }

    @Test
    public void checkpointingIsDisabledIfIntervalIsMaxValue() {
        final StreamGraph jobGraph =
                JobGraphBuilder.newStreamingJobGraphBuilder()
                        .setJobCheckpointingSettings(
                                createCheckpointSettingsWithInterval(Long.MAX_VALUE))
                        .build();

        assertFalse(jobGraph.isCheckpointingEnabled());
    }

    private static JobCheckpointingSettings createCheckpointSettingsWithInterval(
            final long checkpointInterval) {
        final CheckpointCoordinatorConfiguration checkpointCoordinatorConfiguration =
                new CheckpointCoordinatorConfiguration(
                        checkpointInterval,
                        Long.MAX_VALUE,
                        Long.MAX_VALUE,
                        Integer.MAX_VALUE,
                        CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
                        true,
                        false,
                        0,
                        0);

        return new JobCheckpointingSettings(checkpointCoordinatorConfiguration, null);
    }

    @Test
    public void testGetSlotSharingGroups() {
        final StreamVertex v1 = new StreamVertex("1");
        final StreamVertex v2 = new StreamVertex("2");
        final StreamVertex v3 = new StreamVertex("3");
        final StreamVertex v4 = new StreamVertex("4");

        final SlotSharingGroup group1 = new SlotSharingGroup();
        v1.setSlotSharingGroup(group1);
        v2.setSlotSharingGroup(group1);

        final SlotSharingGroup group2 = new SlotSharingGroup();
        v3.setSlotSharingGroup(group2);
        v4.setSlotSharingGroup(group2);

        final StreamGraph jobGraph =
                JobGraphBuilder.newStreamingJobGraphBuilder()
                        .addJobVertices(Arrays.asList(v1, v2, v3, v4))
                        .build();

        assertThat(jobGraph.getSlotSharingGroups(), containsInAnyOrder(group1, group2));
    }

    @Test
    public void testGetCoLocationGroups() {
        final StreamVertex v1 = new StreamVertex("1");
        final StreamVertex v2 = new StreamVertex("2");
        final StreamVertex v3 = new StreamVertex("3");
        final StreamVertex v4 = new StreamVertex("4");

        final SlotSharingGroup slotSharingGroup = new SlotSharingGroup();
        v1.setSlotSharingGroup(slotSharingGroup);
        v2.setSlotSharingGroup(slotSharingGroup);
        v1.setStrictlyCoLocatedWith(v2);

        final StreamGraph jobGraph =
                JobGraphBuilder.newStreamingJobGraphBuilder()
                        .addJobVertices(Arrays.asList(v1, v2, v3, v4))
                        .build();

        assertThat(jobGraph.getCoLocationGroups(), hasSize(1));

        final CoLocationGroup onlyCoLocationGroup =
                jobGraph.getCoLocationGroups().iterator().next();
        assertThat(onlyCoLocationGroup.getVertexIds(), containsInAnyOrder(v1.getID(), v2.getID()));
    }
}
