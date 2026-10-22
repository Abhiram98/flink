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

package org.apache.flink.runtime.jobmaster.event;

import java.util.Arrays;
import java.util.Random;

/** {@link JobEvent} implementation for testing purposes. */
public class TestingJobEvent implements JobEvent {

    public static final int TYPE_ID = 99999;

    /** Field to distinguish two TestingJobEvent. */
    private final int identifier;

    private final byte[] arr;

    public TestingJobEvent(int identifier) {
        this.identifier = identifier;
        this.arr = new byte[2000];
        new Random(0).nextBytes(arr);
    }

    public int getIdentifier() {
        return identifier;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TestingJobEvent) {
            TestingJobEvent other = (TestingJobEvent) obj;
            return other.identifier == identifier && Arrays.equals(other.arr, arr);
        } else {
            return false;
        }
    }

    @Override
    public int getType() {
        return TYPE_ID;
    }

    @Override
    public String toString() {
        return "TestingJobEvent(identifier=" + identifier + ")";
    }
}
