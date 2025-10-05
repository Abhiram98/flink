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

package org.apache.flink.traces;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link EventBuilder}. */
class SpanBuilderTest {

    @Test
    void testSpanBuilder() {
        Event span1 = new EventBuilder(SpanBuilderTest.class, "testSpanBuilder").build();

        assertThat(span1.getStartTsMillis()).isGreaterThan(0);
        assertThat(span1.getEndTsMillis()).isGreaterThan(0);

        long startTsMillis = System.currentTimeMillis();
        Event span2 =
                new EventBuilder(SpanBuilderTest.class, "testSpanBuilder")
                        .setStartTsMillis(startTsMillis)
                        .build();

        assertThat(span2.getStartTsMillis()).isEqualTo(startTsMillis);
        assertThat(span2.getEndTsMillis()).isEqualTo(startTsMillis);

        long endTsMillis = System.currentTimeMillis();
        Event span3 =
                new EventBuilder(SpanBuilderTest.class, "testSpanBuilder")
                        .setEndTsMillis(endTsMillis)
                        .build();

        assertThat(span3.getStartTsMillis()).isGreaterThan(0);
        assertThat(span3.getEndTsMillis()).isEqualTo(endTsMillis);

        Event span4 =
                new EventBuilder(SpanBuilderTest.class, "testSpanBuilder")
                        .setStartTsMillis(startTsMillis)
                        .setEndTsMillis(endTsMillis)
                        .build();

        assertThat(span4.getStartTsMillis()).isEqualTo(startTsMillis);
        assertThat(span4.getEndTsMillis()).isEqualTo(endTsMillis);
    }
}
