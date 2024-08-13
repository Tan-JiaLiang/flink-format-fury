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

package org.apache.flink.formats.fury;

import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.fury.format.row.binary.BinaryRow;
import org.apache.fury.memory.MemoryBuffer;

import java.io.IOException;

import static java.lang.String.format;
import static org.apache.flink.util.Preconditions.checkNotNull;

@Internal
public class FuryRowDataDeserializationSchema implements DeserializationSchema<RowData> {

    private final RowType rowType;
    private final TypeInformation<RowData> resultTypeInfo;
    private final boolean ignoreParseErrors;
    private transient BinaryRow reuseRow;
    private transient BinaryRowToRowDataConverters.BinaryRowToRowDataConverter converter;

    public FuryRowDataDeserializationSchema(RowType rowType,
                                            TypeInformation<RowData> resultTypeInfo,
                                            boolean ignoreParseErrors) {
        this.rowType = rowType;
        this.resultTypeInfo = checkNotNull(resultTypeInfo);
        this.ignoreParseErrors = ignoreParseErrors;
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        this.converter = BinaryRowToRowDataConverters.createConverter(rowType);
        this.reuseRow = new BinaryRow(new Schema(FurySchemaConverter.convertToSchema(rowType).getChildren()));
    }

    @Override
    public RowData deserialize(byte[] message) throws IOException {
        if (message == null) {
            return null;
        }

        try {
            MemoryBuffer buffer = MemoryBuffer.fromByteArray(message);
            reuseRow.pointTo(buffer, 0, buffer.size());
            return (RowData) converter.convert(reuseRow);
        } catch (Throwable t) {
            if (ignoreParseErrors) {
                return null;
            }
            throw new IOException(format("Failed to deserialize FURY '%s'.", new String(message)), t);
        }
    }

    @Override
    public boolean isEndOfStream(RowData nextElement) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return resultTypeInfo;
    }
}
