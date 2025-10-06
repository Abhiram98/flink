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

package org.apache.flink.runtime.jobgraph.jsonplan;

import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.*;
import org.apache.flink.runtime.jobgraph.StreamVertex;
import org.apache.flink.runtime.operators.testutils.DummyInvokable;
import org.apache.flink.util.jackson.JacksonMapperFactory;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.TextNode;

import org.junit.Test;

import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class JsonGeneratorTest {

    @Test
    public void testGeneratorWithoutAnyAttachements() {
        try {
            StreamVertex source1 = new StreamVertex("source 1");

            StreamVertex source2 = new StreamVertex("source 2");
            source2.setInvokableClass(DummyInvokable.class);

            StreamVertex source3 = new StreamVertex("source 3");

            StreamVertex intermediate1 = new StreamVertex("intermediate 1");
            StreamVertex intermediate2 = new StreamVertex("intermediate 2");

            StreamVertex join1 = new StreamVertex("join 1");
            StreamVertex join2 = new StreamVertex("join 2");

            StreamVertex sink1 = new StreamVertex("sink 1");
            StreamVertex sink2 = new StreamVertex("sink 2");

            intermediate1.connectNewDataSetAsInput(
                    source1, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
            intermediate2.connectNewDataSetAsInput(
                    source2, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

            join1.connectNewDataSetAsInput(
                    intermediate1, DistributionPattern.POINTWISE, ResultPartitionType.BLOCKING);
            join1.connectNewDataSetAsInput(
                    intermediate2, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);

            join2.connectNewDataSetAsInput(
                    join1, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
            join2.connectNewDataSetAsInput(
                    source3, DistributionPattern.POINTWISE, ResultPartitionType.BLOCKING);

            sink1.connectNewDataSetAsInput(
                    join2, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
            sink2.connectNewDataSetAsInput(
                    join1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

            StreamGraph jg =
                    StreamGraphTestUtils.batchJobGraph(
                            source1,
                            source2,
                            source3,
                            intermediate1,
                            intermediate2,
                            join1,
                            join2,
                            sink1,
                            sink2);

            String plan = JsonPlanGenerator.generatePlan(jg);
            assertNotNull(plan);

            // validate the produced JSON
            ObjectMapper m = JacksonMapperFactory.createObjectMapper();
            JsonNode rootNode = m.readTree(plan);

            // core fields
            assertEquals(new TextNode(jg.getStreamID().toString()), rootNode.get("jid"));
            assertEquals(new TextNode(jg.getName()), rootNode.get("name"));
            assertEquals(new TextNode(jg.getJobType().name()), rootNode.get("type"));

            assertTrue(rootNode.path("nodes").isArray());

            for (Iterator<JsonNode> iter = rootNode.path("nodes").elements(); iter.hasNext(); ) {
                JsonNode next = iter.next();

                JsonNode idNode = next.get("id");
                assertNotNull(idNode);
                assertTrue(idNode.isTextual());
                checkVertexExists(idNode.asText(), jg);

                String description = next.get("description").asText();
                assertTrue(
                        description.startsWith("source")
                                || description.startsWith("sink")
                                || description.startsWith("intermediate")
                                || description.startsWith("join"));
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    private void checkVertexExists(String vertexId, StreamGraph graph) {
        // validate that the vertex has a valid
        StreamVertexID id = StreamVertexID.fromHexString(vertexId);
        for (StreamVertex vertex : graph.getVertices()) {
            if (vertex.getID().equals(id)) {
                return;
            }
        }
        fail("could not find vertex with id " + vertexId + " in JobGraph");
    }
}
