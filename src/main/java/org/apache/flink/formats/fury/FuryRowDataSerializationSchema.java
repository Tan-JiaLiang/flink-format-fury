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

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.fury.format.row.binary.BinaryRow;
import org.apache.fury.format.row.binary.writer.BinaryRowWriter;

import java.util.Collections;

@Internal
public class FuryRowDataSerializationSchema implements SerializationSchema<RowData> {

    private final RowType rowType;
    private transient RowDataToBinaryRowConverters.RowDataToBinaryRowFiller filler;
    private transient Schema schema;

    public FuryRowDataSerializationSchema(RowType rowType) {
        this.rowType = rowType;
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        Field field = FurySchemaConverter.convertToSchema(rowType);
        this.schema = new Schema(Collections.singletonList(field));
        filler = RowDataToBinaryRowConverters.createFiller(rowType);
    }

    @Override
    public byte[] serialize(RowData input) {
        BinaryRowWriter writer = new BinaryRowWriter(schema);
        writer.reset();
        filler.fill(writer, 0, input);
        BinaryRow output = writer.getRow().getStruct(0);
        return output.toBytes();
    }
}
