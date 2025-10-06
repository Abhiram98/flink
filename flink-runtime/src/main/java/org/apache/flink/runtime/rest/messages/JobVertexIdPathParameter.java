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

package org.apache.flink.runtime.rest.messages;

import org.apache.flink.runtime.jobgraph.StreamVertexID;

/** Path parameter specifying a {@link StreamVertexID}. */
public class JobVertexIdPathParameter extends MessagePathParameter<StreamVertexID> {

    public static final String KEY = "vertexid";

    public JobVertexIdPathParameter() {
        super(KEY);
    }

    @Override
    protected StreamVertexID convertFromString(String value) throws ConversionException {
        return StreamVertexID.fromHexString(value);
    }

    @Override
    protected String convertToString(StreamVertexID value) {
        return value.toString();
    }

    @Override
    public String getDescription() {
        return "32-character hexadecimal string value that identifies a job vertex.";
    }
}
