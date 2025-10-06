/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.dispatcher.runner;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.dispatcher.JobDispatcherFactory;
import org.apache.flink.runtime.dispatcher.PartialDispatcherServices;
import org.apache.flink.runtime.entrypoint.component.JobGraphRetriever;
import org.apache.flink.runtime.highavailability.StreamResultStore;
import org.apache.flink.runtime.jobgraph.StreamGraph;
import org.apache.flink.runtime.jobmanager.JobPersistenceComponentFactory;
import org.apache.flink.runtime.jobmaster.StreamResult;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

/** Factory for the {@link JobDispatcherLeaderProcessFactory}. */
public class JobDispatcherLeaderProcessFactoryFactory
        implements DispatcherLeaderProcessFactoryFactory {

    private static final Logger LOG =
            LoggerFactory.getLogger(JobDispatcherLeaderProcessFactoryFactory.class);

    private final JobGraphRetriever jobGraphRetriever;

    @VisibleForTesting
    JobDispatcherLeaderProcessFactoryFactory(JobGraphRetriever jobGraphRetriever) {
        this.jobGraphRetriever = jobGraphRetriever;
    }

    @Override
    public JobDispatcherLeaderProcessFactory createFactory(
            JobPersistenceComponentFactory jobPersistenceComponentFactory,
            Executor ioExecutor,
            RpcService rpcService,
            PartialDispatcherServices partialDispatcherServices,
            FatalErrorHandler fatalErrorHandler) {

        final StreamGraph jobGraph;

        try {
            jobGraph =
                    Preconditions.checkNotNull(
                            jobGraphRetriever.retrieveJobGraph(
                                    partialDispatcherServices.getConfiguration()));
        } catch (FlinkException e) {
            throw new FlinkRuntimeException("Could not retrieve the JobGraph.", e);
        }

        final StreamResultStore jobResultStore = jobPersistenceComponentFactory.createJobResultStore();
        final Collection<StreamResult> recoveredDirtyJobResults = getDirtyJobResults(jobResultStore);

        final Optional<StreamResult> maybeRecoveredDirtyJobResult =
                extractDirtyJobResult(recoveredDirtyJobResults, jobGraph);
        final Optional<StreamGraph> maybeJobGraph =
                getJobGraphBasedOnDirtyJobResults(jobGraph, recoveredDirtyJobResults);

        final DefaultDispatcherGatewayServiceFactory defaultDispatcherServiceFactory =
                new DefaultDispatcherGatewayServiceFactory(
                        JobDispatcherFactory.INSTANCE, rpcService, partialDispatcherServices);

        return new JobDispatcherLeaderProcessFactory(
                defaultDispatcherServiceFactory,
                maybeJobGraph.orElse(null),
                maybeRecoveredDirtyJobResult.orElse(null),
                jobResultStore,
                fatalErrorHandler);
    }

    public static JobDispatcherLeaderProcessFactoryFactory create(
            JobGraphRetriever jobGraphRetriever) {
        return new JobDispatcherLeaderProcessFactoryFactory(jobGraphRetriever);
    }

    private static Collection<StreamResult> getDirtyJobResults(StreamResultStore jobResultStore) {
        try {
            return jobResultStore.getDirtyResults();
        } catch (IOException e) {
            throw new FlinkRuntimeException(
                    "Could not retrieve the JobResults of dirty jobs from the underlying JobResultStore.",
                    e);
        }
    }

    private static Optional<StreamResult> extractDirtyJobResult(
            Collection<StreamResult> dirtyJobResults, StreamGraph jobGraph) {
        Optional<StreamResult> actualDirtyJobResult = Optional.empty();
        for (StreamResult dirtyJobResult : dirtyJobResults) {
            if (dirtyJobResult.getStreamId().equals(jobGraph.getStreamID())) {
                actualDirtyJobResult = Optional.of(dirtyJobResult);
            } else {
                LOG.warn(
                        "Unexpected dirty JobResultStore entry: Job '{}' is listed as dirty, isn't part of this single-job cluster, though.",
                        dirtyJobResult.getStreamId());
            }
        }

        return actualDirtyJobResult;
    }

    private static Optional<StreamGraph> getJobGraphBasedOnDirtyJobResults(
            StreamGraph jobGraph, Collection<StreamResult> dirtyJobResults) {
        final Set<JobID> jobIdsOfFinishedJobs =
                dirtyJobResults.stream().map(StreamResult::getStreamId).collect(Collectors.toSet());
        if (jobIdsOfFinishedJobs.contains(jobGraph.getStreamID())) {
            LOG.info(
                    "Skipping recovery of a job with job id {}, because it already reached a globally terminal state",
                    jobGraph.getStreamID());
            return Optional.empty();
        }
        return Optional.of(jobGraph);
    }
}
