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

package org.apache.flink.runtime.highavailability;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.testutils.TestingJobResultStore;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

/**
 * This interface defines a series of tests for any implementation of the {@link StreamResultStore} to
 * determine whether they correctly implement the contracts defined by the interface.
 */
public interface StreamResultStoreContractTest {

    StreamResultEntry DUMMY_STREAM_RESULT_ENTRY =
            new StreamResultEntry(TestingJobResultStore.DUMMY_JOB_RESULT);

    StreamResultStore createStreamResultStore() throws IOException;

    @Test
    default void testStoreStreamResultsWithDuplicateIDsThrowsException() throws IOException {
        StreamResultStore streamResultStore = createStreamResultStore();
        streamResultStore.createDirtyResultAsync(DUMMY_STREAM_RESULT_ENTRY).join();
        final StreamResultEntry otherEntryWithDuplicateId =
                new StreamResultEntry(
                        TestingJobResultStore.createSuccessfulJobResult(
                                DUMMY_STREAM_RESULT_ENTRY.getStreamId()));
        assertThatThrownBy(
                        () ->
                                streamResultStore
                                        .createDirtyResultAsync(otherEntryWithDuplicateId)
                                        .join())
                .isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(IllegalStateException.class);
    }

    @Test
    default void testStoreDirtyEntryForAlreadyCleanedStreamResultThrowsException() throws IOException {
        StreamResultStore streamResultStore = createStreamResultStore();
        streamResultStore.createDirtyResultAsync(DUMMY_STREAM_RESULT_ENTRY).join();
        streamResultStore.markResultAsCleanAsync(DUMMY_STREAM_RESULT_ENTRY.getStreamId()).join();
        assertThatThrownBy(
                        () -> streamResultStore.createDirtyResultAsync(DUMMY_STREAM_RESULT_ENTRY).join())
                .isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(IllegalStateException.class);
    }

    @Test
    default void testCleaningDuplicateEntryThrowsNoException() throws IOException {
        StreamResultStore streamResultStore = createStreamResultStore();
        streamResultStore.createDirtyResultAsync(DUMMY_STREAM_RESULT_ENTRY).join();
        streamResultStore.markResultAsCleanAsync(DUMMY_STREAM_RESULT_ENTRY.getStreamId()).join();
        assertThatNoException()
                .isThrownBy(
                        () ->
                                streamResultStore
                                        .markResultAsCleanAsync(DUMMY_STREAM_RESULT_ENTRY.getStreamId())
                                        .join());
    }

    @Test
    default void testCleaningNonExistentEntryThrowsException() throws IOException {
        StreamResultStore streamResultStore = createStreamResultStore();
        assertThatThrownBy(
                        () ->
                                streamResultStore
                                        .markResultAsCleanAsync(DUMMY_STREAM_RESULT_ENTRY.getStreamId())
                                        .join())
                .hasCauseInstanceOf(NoSuchElementException.class);
    }

    @Test
    default void testHasStreamResultEntryWithDirtyEntry() throws IOException {
        StreamResultStore streamResultStore = createStreamResultStore();
        streamResultStore.createDirtyResultAsync(DUMMY_STREAM_RESULT_ENTRY).join();
        assertThat(
                        streamResultStore
                                .hasDirtyStreamResultEntryAsync(DUMMY_STREAM_RESULT_ENTRY.getStreamId())
                                .join())
                .isTrue();
        assertThat(
                        streamResultStore
                                .hasCleanStreamResultEntryAsync(DUMMY_STREAM_RESULT_ENTRY.getStreamId())
                                .join())
                .isFalse();
        assertThat(streamResultStore
                .hasStreamResultEntryAsync(DUMMY_STREAM_RESULT_ENTRY.getStreamId()).join())
                .isTrue();
    }

    @Test
    default void testHasStreamResultEntryWithCleanEntry() throws IOException {
        StreamResultStore streamResultStore = createStreamResultStore();
        streamResultStore.createDirtyResultAsync(DUMMY_STREAM_RESULT_ENTRY).join();
        streamResultStore.markResultAsCleanAsync(DUMMY_STREAM_RESULT_ENTRY.getStreamId()).join();
        assertThat(
                        streamResultStore
                                .hasDirtyStreamResultEntryAsync(DUMMY_STREAM_RESULT_ENTRY.getStreamId())
                                .join())
                .isFalse();
        assertThat(
                        streamResultStore
                                .hasCleanStreamResultEntryAsync(DUMMY_STREAM_RESULT_ENTRY.getStreamId())
                                .join())
                .isTrue();
        assertThat(streamResultStore
                .hasStreamResultEntryAsync(DUMMY_STREAM_RESULT_ENTRY.getStreamId()).join())
                .isTrue();
    }

    @Test
    default void testHasStreamResultEntryWithEmptyStore() throws IOException {
        StreamResultStore streamResultStore = createStreamResultStore();
        JobID streamId = new JobID();
        assertThat(streamResultStore.hasDirtyStreamResultEntryAsync(streamId).join()).isFalse();
        assertThat(streamResultStore.hasCleanStreamResultEntryAsync(streamId).join()).isFalse();
        assertThat(streamResultStore.hasStreamResultEntryAsync(streamId).join()).isFalse();
    }

    @Test
    default void testGetDirtyResultsWithNoEntry() throws IOException {
        StreamResultStore streamResultStore = createStreamResultStore();
        assertThat(streamResultStore.getDirtyResults()).isEmpty();
    }

    @Test
    default void testGetDirtyResultsWithDirtyEntry() throws IOException {
        StreamResultStore streamResultStore = createStreamResultStore();
        streamResultStore.createDirtyResultAsync(DUMMY_STREAM_RESULT_ENTRY).join();
        assertThat(
                        streamResultStore.getDirtyResults().stream()
                                .map(JobResult::getStreamId)
                                .collect(Collectors.toList()))
                .singleElement()
                .isEqualTo(DUMMY_STREAM_RESULT_ENTRY.getStreamId());
    }

    @Test
    default void testGetDirtyResultsWithDirtyAndCleanEntry() throws IOException {
        StreamResultStore streamResultStore = createStreamResultStore();
        streamResultStore.createDirtyResultAsync(DUMMY_STREAM_RESULT_ENTRY).join();
        streamResultStore.markResultAsCleanAsync(DUMMY_STREAM_RESULT_ENTRY.getStreamId()).join();

        final StreamResultEntry otherDirtyStreamResultEntry =
                new StreamResultEntry(TestingJobResultStore.createSuccessfulJobResult(new JobID()));
        streamResultStore.createDirtyResultAsync(otherDirtyStreamResultEntry).join();

        assertThat(
                        streamResultStore.getDirtyResults().stream()
                                .map(JobResult::getStreamId)
                                .collect(Collectors.toList()))
                .singleElement()
                .isEqualTo(otherDirtyStreamResultEntry.getStreamId());
    }
}
