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

package org.apache.flink.runtime;

import org.apache.flink.runtime.jobgraph.VertexID;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Optional;

/** Formed of a mandatory operator ID and optionally a user defined operator ID. */
public class OperatorIDPair implements Serializable {

    private static final long serialVersionUID = 1L;

    private final VertexID generatedOperatorID;
    @Nullable private final VertexID userDefinedOperatorID;
    @Nullable private final String userDefinedOperatorName;
    @Nullable private final String userDefinedOperatorUid;

    private OperatorIDPair(
            VertexID generatedOperatorID,
            @Nullable VertexID userDefinedOperatorID,
            @Nullable String userDefinedOperatorName,
            @Nullable String userDefinedOperatorUid) {
        this.generatedOperatorID = generatedOperatorID;
        this.userDefinedOperatorID = userDefinedOperatorID;
        if (userDefinedOperatorName != null && userDefinedOperatorName.isEmpty()) {
            throw new IllegalArgumentException("Empty string operator name is not allowed");
        }
        this.userDefinedOperatorName = userDefinedOperatorName;
        if (userDefinedOperatorUid != null && userDefinedOperatorUid.isEmpty()) {
            throw new IllegalArgumentException("Empty string operator uid is not allowed");
        }
        this.userDefinedOperatorUid = userDefinedOperatorUid;
    }

    public static OperatorIDPair of(
            VertexID generatedOperatorID,
            @Nullable VertexID userDefinedOperatorID,
            @Nullable String operatorName,
            @Nullable String operatorUid) {
        return new OperatorIDPair(
                generatedOperatorID, userDefinedOperatorID, operatorName, operatorUid);
    }

    public static OperatorIDPair generatedIDOnly(VertexID generatedOperatorID) {
        return new OperatorIDPair(generatedOperatorID, null, null, null);
    }

    public VertexID getGeneratedOperatorID() {
        return generatedOperatorID;
    }

    @Nullable
    public Optional<VertexID> getUserDefinedOperatorID() {
        return Optional.ofNullable(userDefinedOperatorID);
    }

    @Nullable
    public String getUserDefinedOperatorName() {
        return userDefinedOperatorName;
    }

    @Nullable
    public String getUserDefinedOperatorUid() {
        return userDefinedOperatorUid;
    }
}
