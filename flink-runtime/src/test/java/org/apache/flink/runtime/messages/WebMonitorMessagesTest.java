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

package org.apache.flink.runtime.messages;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.messages.webmonitor.ClusterOverview;
import org.apache.flink.runtime.messages.webmonitor.RequestStreamsOverview;
import org.apache.flink.runtime.messages.webmonitor.StreamDetails;
import org.apache.flink.runtime.messages.webmonitor.StreamIdsWithStatusOverview;
import org.apache.flink.runtime.messages.webmonitor.StreamsOverview;
import org.apache.flink.runtime.messages.webmonitor.MultipleStreamsDetails;
import org.apache.flink.runtime.messages.webmonitor.RequestStreamDetails;
import org.apache.flink.runtime.messages.webmonitor.RequestStreamsWithIDsOverview;
import org.apache.flink.runtime.messages.webmonitor.RequestStatusOverview;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

class WebMonitorMessagesTest {

    @Test
    void testStatusMessages() throws Exception {
        final Random rnd = new Random();

        GenericMessageTester.testMessageInstance(RequestStreamsOverview.getInstance());
        GenericMessageTester.testMessageInstance(RequestStreamsWithIDsOverview.getInstance());
        GenericMessageTester.testMessageInstance(RequestStatusOverview.getInstance());
        GenericMessageTester.testMessageInstance(RequestStreamsOverview.getInstance());

        GenericMessageTester.testMessageInstance(
                GenericMessageTester.instantiateGeneric(RequestStreamDetails.class, rnd));
        GenericMessageTester.testMessageInstance(
                GenericMessageTester.instantiateGeneric(ClusterOverview.class, rnd));
        GenericMessageTester.testMessageInstance(
                GenericMessageTester.instantiateGeneric(StreamsOverview.class, rnd));

        GenericMessageTester.testMessageInstance(
                new StreamIdsWithStatusOverview(
                        Arrays.asList(
                                new StreamIdsWithStatusOverview.StreamIdWithStatus(
                                        JobID.generate(), JobStatus.RUNNING),
                                new StreamIdsWithStatusOverview.StreamIdWithStatus(
                                        JobID.generate(), JobStatus.CANCELED),
                                new StreamIdsWithStatusOverview.StreamIdWithStatus(
                                        JobID.generate(), JobStatus.CREATED),
                                new StreamIdsWithStatusOverview.StreamIdWithStatus(
                                        JobID.generate(), JobStatus.FAILED),
                                new StreamIdsWithStatusOverview.StreamIdWithStatus(
                                        JobID.generate(), JobStatus.RESTARTING))));
    }

    @Test
    void testStreamDetailsMessage() throws Exception {
        final Random rnd = new Random();

        int[] numVerticesPerState = new int[ExecutionState.values().length];
        int numTotal = 0;

        for (int i = 0; i < numVerticesPerState.length; i++) {
            int count = rnd.nextInt(55);
            numVerticesPerState[i] = count;
            numTotal += count;
        }

        long time = rnd.nextLong();
        long endTime = rnd.nextBoolean() ? -1L : time + rnd.nextInt();
        long lastModified = endTime == -1 ? time + rnd.nextInt() : endTime;

        String name = GenericMessageTester.randomString(rnd);
        JobID jid = GenericMessageTester.randomJobId(rnd);
        JobStatus status = GenericMessageTester.randomJobStatus(rnd);

        StreamDetails msg1 =
                new StreamDetails(
                        jid,
                        name,
                        time,
                        endTime,
                        endTime - time,
                        status,
                        lastModified,
                        numVerticesPerState,
                        numTotal);
        StreamDetails msg2 =
                new StreamDetails(
                        jid,
                        name,
                        time,
                        endTime,
                        endTime - time,
                        status,
                        lastModified,
                        numVerticesPerState,
                        numTotal);

        GenericMessageTester.testMessageInstances(msg1, msg2);
    }

    @Test
    void testMultipleStreamDetails() throws Exception {
        final Random rnd = new Random();
        GenericMessageTester.testMessageInstance(new MultipleStreamsDetails(randomStreamDetails(rnd)));
    }

    private Collection<StreamDetails> randomStreamDetails(Random rnd) {
        final StreamDetails[] details = new StreamDetails[rnd.nextInt(10)];
        for (int k = 0; k < details.length; k++) {
            int[] numVerticesPerState = new int[ExecutionState.values().length];
            int numTotal = 0;

            for (int i = 0; i < numVerticesPerState.length; i++) {
                int count = rnd.nextInt(55);
                numVerticesPerState[i] = count;
                numTotal += count;
            }

            long time = rnd.nextLong();
            long endTime = rnd.nextBoolean() ? -1L : time + rnd.nextInt();
            long lastModified = endTime == -1 ? time + rnd.nextInt() : endTime;

            String name = new GenericMessageTester.StringInstantiator().instantiate(rnd);
            JobID jid = new JobID();
            JobStatus status = JobStatus.values()[rnd.nextInt(JobStatus.values().length)];

            details[k] =
                    new StreamDetails(
                            jid,
                            name,
                            time,
                            endTime,
                            endTime - time,
                            status,
                            lastModified,
                            numVerticesPerState,
                            numTotal);
        }
        return Arrays.asList(details);
    }
}
