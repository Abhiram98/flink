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

package org.apache.flink.formats.parquet.vector;

import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.columnar.vector.Dictionary;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;

import static org.apache.flink.formats.parquet.vector.reader.TimestampColumnReader.decodeInt64ToTimestamp;
import static org.apache.flink.formats.parquet.vector.reader.TimestampColumnReader.decodeInt96ToTimestamp;

/** Parquet dictionary. */
public final class ParquetDictionary implements Dictionary {

    private org.apache.parquet.column.Dictionary dictionary;
    private final ColumnDescriptor descriptor;

    public ParquetDictionary(
            org.apache.parquet.column.Dictionary dictionary, ColumnDescriptor descriptor) {
        this.dictionary = dictionary;
        this.descriptor = descriptor;
    }

    @Override
    public int decodeToInt(int identifier) {
        return dictionary.decodeToInt(identifier);
    }

    @Override
    public long decodeToLong(int identifier) {
        return dictionary.decodeToLong(identifier);
    }

    @Override
    public float decodeToFloat(int identifier) {
        return dictionary.decodeToFloat(identifier);
    }

    @Override
    public double decodeToDouble(int identifier) {
        return dictionary.decodeToDouble(identifier);
    }

    @Override
    public byte[] decodeToBinary(int identifier) {
        return dictionary.decodeToBinary(identifier).getBytesUnsafe();
    }

    @Override
    public TimestampData decodeToTimestamp(int identifier) {
        if (descriptor.getPrimitiveType().getPrimitiveTypeName()
                == PrimitiveType.PrimitiveTypeName.INT64) {
            return decodeInt64ToTimestamp(
                    true,
                    dictionary,
                    identifier,
                    ((LogicalTypeAnnotation.TimestampLogicalTypeAnnotation)
                                    descriptor.getPrimitiveType().getLogicalTypeAnnotation())
                            .getUnit());
        }
        return decodeInt96ToTimestamp(true, dictionary, identifier);
    }
}
