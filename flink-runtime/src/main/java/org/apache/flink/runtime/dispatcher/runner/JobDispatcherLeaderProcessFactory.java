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

import org.apache.flink.runtime.highavailability.StreamResultStore;
import org.apache.flink.runtime.jobgraph.StreamGraph;
import org.apache.flink.runtime.jobmaster.StreamResult;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.UUID;

/** Factory for the {@link JobDispatcherLeaderProcess}. */
public class JobDispatcherLeaderProcessFactory implements DispatcherLeaderProcessFactory {

    private final AbstractDispatcherLeaderProcess.DispatcherGatewayServiceFactory
            dispatcherGatewayServiceFactory;

    @Nullable private final StreamGraph jobGraph;
    @Nullable private final StreamResult recoveredDirtyJobResult;

    private final StreamResultStore jobResultStore;

    private final FatalErrorHandler fatalErrorHandler;

    JobDispatcherLeaderProcessFactory(
            AbstractDispatcherLeaderProcess.DispatcherGatewayServiceFactory
                    dispatcherGatewayServiceFactory,
            @Nullable StreamGraph jobGraph,
            @Nullable StreamResult recoveredDirtyJobResult,
            StreamResultStore jobResultStore,
            FatalErrorHandler fatalErrorHandler) {
        this.dispatcherGatewayServiceFactory = dispatcherGatewayServiceFactory;
        this.jobGraph = jobGraph;
        this.recoveredDirtyJobResult = recoveredDirtyJobResult;
        this.jobResultStore = Preconditions.checkNotNull(jobResultStore);
        this.fatalErrorHandler = fatalErrorHandler;
    }

    @Override
    public DispatcherLeaderProcess create(UUID leaderSessionID) {
        return new JobDispatcherLeaderProcess(
                leaderSessionID,
                dispatcherGatewayServiceFactory,
                jobGraph,
                recoveredDirtyJobResult,
                jobResultStore,
                fatalErrorHandler);
    }

    @Nullable
    StreamGraph getJobGraph() {
        return this.jobGraph;
    }

    @Nullable
    StreamResult getRecoveredDirtyJobResult() {
        return this.recoveredDirtyJobResult;
    }
}
