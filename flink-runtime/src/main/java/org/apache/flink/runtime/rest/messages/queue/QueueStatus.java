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

package org.apache.flink.runtime.rest.messages.queue;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import static java.util.Objects.requireNonNull;

/**
 * Response type for temporary queue resources, i.e., resources that are asynchronously created.
 *
 * @see org.apache.flink.runtime.rest.handler.job.JobExecutionResultHandler
 */
public class QueueStatus {

    private static final String FIELD_NAME_ID = "identifier";

    @JsonProperty(value = FIELD_NAME_ID, required = true)
    private final Id identifier;

    @JsonCreator
    public QueueStatus(@JsonProperty(value = FIELD_NAME_ID, required = true) final Id identifier) {
        this.identifier = requireNonNull(identifier, "statusId must not be null");
    }

    public static QueueStatus inProgress() {
        return new QueueStatus(Id.IN_PROGRESS);
    }

    public static QueueStatus completed() {
        return new QueueStatus(Id.COMPLETED);
    }

    public Id getIdentifier() {
        return identifier;
    }

    /** Defines queue statuses. */
    public enum Id {
        IN_PROGRESS,
        COMPLETED
    }
}
