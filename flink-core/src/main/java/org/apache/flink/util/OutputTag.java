/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.util;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;

/**
 * An {@link OutputTag} is a typed and named tag to use for tagging side outputs of an operator.
 *
 * <p>An {@code OutputTag} must always be an anonymous inner class so that Flink can derive a {@link
 * TypeInformation} for the generic type parameter.
 *
 * <p>Example:
 *
 * <pre>{@code
 * OutputTag<Tuple2<String, Long>> info = new OutputTag<Tuple2<String, Long>>("late-data"){};
 * }</pre>
 *
 * @param <T> the type of elements in the side-output stream.
 */
@PublicEvolving
public class OutputTag<T> implements Serializable {

    private static final long serialVersionUID = 2L;

    private final String identifier;

    private final TypeInformation<T> typeInfo;

    /**
     * Creates a new named {@code OutputTag} with the given identifier.
     *
     * @param identifier The identifier of the created {@code OutputTag}.
     */
    public OutputTag(String identifier) {
        Preconditions.checkNotNull(identifier, "OutputTag identifier cannot be null.");
        Preconditions.checkArgument(!identifier.isEmpty(), "OutputTag identifier must not be empty.");
        this.identifier = identifier;

        try {
            this.typeInfo = TypeExtractor.createTypeInfo(this, OutputTag.class, getClass(), 0);
        } catch (InvalidTypesException e) {
            throw new InvalidTypesException(
                    "Could not determine TypeInformation for the OutputTag type. "
                            + "The most common reason is forgetting to make the OutputTag an anonymous inner class. "
                            + "It is also not possible to use generic type variables with OutputTags, such as 'Tuple2<A, B>'.",
                    e);
        }
    }

    /**
     * Creates a new named {@code OutputTag} with the given identifier and output {@link TypeInformation}.
     *
     * @param identifier The identifier of the created {@code OutputTag}.
     * @param typeInfo The {@code TypeInformation} for the side output.
     */
    public OutputTag(String identifier, TypeInformation<T> typeInfo) {
        Preconditions.checkNotNull(identifier, "OutputTag identifier cannot be null.");
        Preconditions.checkArgument(!identifier.isEmpty(), "OutputTag identifier must not be empty.");
        this.identifier = identifier;
        this.typeInfo = Preconditions.checkNotNull(typeInfo, "TypeInformation cannot be null.");
    }

    public static boolean isResponsibleFor(
            @Nullable OutputTag<?> owner, @Nonnull OutputTag<?> other) {
        return other.equals(owner);
    }

    // ------------------------------------------------------------------------

    public String getIdentifier() {
        return identifier;
    }

    public TypeInformation<T> getTypeInfo() {
        return typeInfo;
    }

    // ------------------------------------------------------------------------

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null || !(obj instanceof OutputTag)) {
            return false;
        }
        OutputTag other = (OutputTag) obj;
        return Objects.equals(this.identifier, other.identifier);
    }

    @Override
    public int hashCode() {
        return identifier.hashCode();
    }

    @Override
    public String toString() {
        return "OutputTag(" + getTypeInfo() + ", " + identifier + ")";
    }
}
