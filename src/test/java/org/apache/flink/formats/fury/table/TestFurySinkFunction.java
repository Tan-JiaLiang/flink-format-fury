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

package org.apache.flink.formats.fury.table;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.RowData;

/** Sink Function for fury table factory test. Must run in single parallelism. */
public class TestFurySinkFunction extends RichSinkFunction<RowData> {
    private final SerializationSchema<RowData> serializer;

    public TestFurySinkFunction(SerializationSchema<RowData> serializer) {
        this.serializer = serializer;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.serializer.open(null);
    }

    @Override
    public void invoke(RowData value, Context context) throws Exception {
        byte[] bytes = serializer.serialize(value);
        TestFuryTestStore.sinkResults.add(bytes);
    }
}
