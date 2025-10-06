/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.jobgraph.forwardgroup;

import org.apache.flink.runtime.executiongraph.VertexGroupComputeUtil;
import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.StreamVertex;
import org.apache.flink.runtime.jobgraph.StreamVertexID;

import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkState;

/** Common utils for computing forward groups. */
public class ForwardGroupComputeUtil {

    public static Map<StreamVertexID, ForwardGroup> computeForwardGroupsAndCheckParallelism(
            final Iterable<StreamVertex> topologicallySortedVertices) {
        final Map<StreamVertexID, ForwardGroup> forwardGroupsByJobVertexId =
                computeForwardGroups(
                        topologicallySortedVertices, ForwardGroupComputeUtil::getForwardProducers);
        // the vertex's parallelism in parallelism-decided forward group should have been set at
        // compilation phase
        topologicallySortedVertices.forEach(
                jobVertex -> {
                    ForwardGroup forwardGroup = forwardGroupsByJobVertexId.get(jobVertex.getID());
                    if (forwardGroup != null && forwardGroup.isParallelismDecided()) {
                        checkState(jobVertex.getParallelism() == forwardGroup.getParallelism());
                    }
                });
        return forwardGroupsByJobVertexId;
    }

    public static Map<StreamVertexID, ForwardGroup> computeForwardGroups(
            final Iterable<StreamVertex> topologicallySortedVertices,
            final Function<StreamVertex, Set<StreamVertex>> forwardProducersRetriever) {

        final Map<StreamVertex, Set<StreamVertex>> vertexToGroup = new IdentityHashMap<>();

        // iterate all the vertices which are topologically sorted
        for (StreamVertex vertex : topologicallySortedVertices) {
            Set<StreamVertex> currentGroup = new HashSet<>();
            currentGroup.add(vertex);
            vertexToGroup.put(vertex, currentGroup);

            for (StreamVertex producerVertex : forwardProducersRetriever.apply(vertex)) {
                final Set<StreamVertex> producerGroup = vertexToGroup.get(producerVertex);

                if (producerGroup == null) {
                    throw new IllegalStateException(
                            "Producer task "
                                    + producerVertex.getID()
                                    + " forward group is null"
                                    + " while calculating forward group for the consumer task "
                                    + vertex.getID()
                                    + ". This should be a forward group building bug.");
                }

                if (currentGroup != producerGroup) {
                    currentGroup =
                            VertexGroupComputeUtil.mergeVertexGroups(
                                    currentGroup, producerGroup, vertexToGroup);
                }
            }
        }

        final Map<StreamVertexID, ForwardGroup> ret = new HashMap<>();
        for (Set<StreamVertex> vertexGroup :
                VertexGroupComputeUtil.uniqueVertexGroups(vertexToGroup)) {
            if (vertexGroup.size() > 1) {
                ForwardGroup forwardGroup = new ForwardGroup(vertexGroup);
                for (StreamVertexID jobVertexId : forwardGroup.getJobVertexIds()) {
                    ret.put(jobVertexId, forwardGroup);
                }
            }
        }
        return ret;
    }

    static Set<StreamVertex> getForwardProducers(final StreamVertex jobVertex) {
        return jobVertex.getInputs().stream()
                .filter(JobEdge::isForward)
                .map(JobEdge::getSource)
                .map(IntermediateDataSet::getProducer)
                .collect(Collectors.toSet());
    }
}
