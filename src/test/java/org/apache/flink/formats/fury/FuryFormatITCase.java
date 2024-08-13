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
import org.apache.flink.formats.fury.table.TestFuryTestStore;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.*;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.runtime.utils.BatchTestBase;
import org.apache.flink.types.Row;
import org.apache.fury.format.row.binary.BinaryMap;
import org.apache.fury.format.row.binary.BinaryRow;
import org.apache.fury.format.row.binary.writer.BinaryArrayWriter;
import org.apache.fury.format.row.binary.writer.BinaryRowWriter;
import org.apache.fury.memory.MemoryBuffer;
import org.junit.Test;

import java.math.BigDecimal;
import java.time.*;
import java.util.Collections;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

/**
 * Integration SQL test for fury.
 */
public class FuryFormatITCase extends BatchTestBase {

    private ResolvedSchema schema() {
        return ResolvedSchema.of(
                Column.physical("f_boolean", DataTypes.BOOLEAN()),
                Column.physical("f_tinyint", DataTypes.TINYINT()),
                Column.physical("f_smallint", DataTypes.SMALLINT()),
                Column.physical("f_int", DataTypes.INT()),
                Column.physical("f_bigint", DataTypes.BIGINT()),
                Column.physical("f_float", DataTypes.FLOAT()),
                Column.physical("f_double", DataTypes.DOUBLE()),
                Column.physical("f_date", DataTypes.DATE()),
                Column.physical("f_interval_year", DataTypes.INTERVAL(DataTypes.YEAR())),
                Column.physical("f_interval_month", DataTypes.INTERVAL(DataTypes.MONTH())),
                Column.physical("f_interval_day", DataTypes.INTERVAL(DataTypes.DAY())),
                Column.physical("f_interval_hour", DataTypes.INTERVAL(DataTypes.HOUR())),
                Column.physical("f_interval_minute", DataTypes.INTERVAL(DataTypes.MINUTE())),
                Column.physical("f_interval_second", DataTypes.INTERVAL(DataTypes.SECOND())),
                Column.physical("f_varbinary", DataTypes.VARBINARY(Integer.MAX_VALUE)),
                Column.physical("f_decimal", DataTypes.DECIMAL(3, 2)),
                Column.physical("f_time", DataTypes.TIME(3)),
                Column.physical("f_timestamp", DataTypes.TIMESTAMP(3)),
                Column.physical("f_timestamp_ltz", DataTypes.TIMESTAMP_LTZ(3)),
                Column.physical("f_map", DataTypes.MAP(DataTypes.STRING().notNull(), DataTypes.STRING())),
                Column.physical("f_array", DataTypes.ARRAY(DataTypes.STRING())),
                Column.physical("f_row", DataTypes.ROW(
                        DataTypes.FIELD("f1", DataTypes.STRING()),
                        DataTypes.FIELD("f2", DataTypes.BIGINT())))
        );
    }

    private Row testRow() {
        return Row.of(
                true,
                (byte) 1,
                (short) 2,
                3,
                4L,
                5.0f,
                6.0d,
                LocalDate.ofEpochDay(7),    // date
                Period.ofMonths(8),    // interval year
                Period.ofMonths(9),  // interval month
                Duration.ofMillis(10L), // interval day
                Duration.ofMillis(11L),    // interval hour
                Duration.ofMillis(12L),  // interval minute
                Duration.ofMillis(13L), // interval second
                new byte[]{1, 2, 3}, // varbinary
                new BigDecimal("1.23"), // decimal
                TimestampData.fromEpochMillis(16).toLocalDateTime().toLocalTime(),   // time
                TimestampData.fromEpochMillis(1723185890468L).toLocalDateTime(), // timestamp
                TimestampData.fromEpochMillis(1723185890468L).toInstant(), // timestamp_ltz
                Collections.singletonMap("hello", "world"), // map
                new String[]{"hello", "world"}, // array
                Row.of("hello", 123L)   // row
        );
    }

    private BinaryRow testBinaryRow() {
        Field field = FurySchemaConverter.convertToSchema(schema().toPhysicalRowDataType().getLogicalType());
        BinaryRowWriter writer = new BinaryRowWriter(new Schema(field.getChildren()));
        writer.reset();

        writer.write(0, true);  // boolean
        writer.write(1, (byte) 1);    // tinyint
        writer.write(2, (short) 2);   // smallint
        writer.write(3, 3);     // int
        writer.write(4, 4L);    // bigint
        writer.write(5, 5.0f);  // float
        writer.write(6, 6.0d);  // double
        writer.write(7, 7);     // date
        writer.write(8, 8);     // interval year
        writer.write(9, 9);     // interval month
        writer.write(10, 10L);  // interval day
        writer.write(11, 11L);  // interval hour
        writer.write(12, 12L);  // interval minute
        writer.write(13, 13L);  // interval second
        writer.write(14, new byte[]{1, 2, 3});  // varbinary
        writer.write(15, new BigDecimal("1.23"));   // decimal
        writer.write(16, 16);   // time
        writer.write(17, 1723185890468L);   // timestamp
        writer.write(18, 1723185890468L);   // timestamp_ltz

        // map
        Field keyField = FurySchemaConverter.convertToSchema(DataTypes.STRING().notNull().getLogicalType());
        BinaryArrayWriter keyArrayWriter = new BinaryArrayWriter(org.apache.fury.format.type.DataTypes.arrayField(keyField));
        keyArrayWriter.reset(1);
        keyArrayWriter.write(0, "hello");

        Field valueField = FurySchemaConverter.convertToSchema(DataTypes.STRING().getLogicalType());
        BinaryArrayWriter valueArrayWriter = new BinaryArrayWriter(org.apache.fury.format.type.DataTypes.arrayField(valueField));
        valueArrayWriter.reset(1);
        valueArrayWriter.write(0, "world");

        Field mapField = org.apache.fury.format.type.DataTypes.mapField("", keyField, valueField);
        BinaryMap binaryMap = new BinaryMap(keyArrayWriter.toArray(), valueArrayWriter.toArray(), mapField);
        writer.write(19, binaryMap);

        // array
        Field arrayField = FurySchemaConverter.convertToSchema(DataTypes.STRING().getLogicalType());
        BinaryArrayWriter arrayWriter = new BinaryArrayWriter(org.apache.fury.format.type.DataTypes.arrayField(arrayField));
        arrayWriter.reset(2);
        arrayWriter.write(0, "hello");
        arrayWriter.write(1, "world");
        writer.write(20, arrayWriter.toArray());

        // row
        Field rowField =
                FurySchemaConverter.convertToSchema(
                        DataTypes.ROW(
                                        DataTypes.FIELD("f1", DataTypes.STRING()),
                                        DataTypes.FIELD("f2", DataTypes.BIGINT()))
                                .getLogicalType());
        BinaryRowWriter rowWriter = new BinaryRowWriter(new Schema(rowField.getChildren()));
        rowWriter.reset();
        rowWriter.write(0, "hello");
        rowWriter.write(1, 123L);
        writer.write(21, rowWriter.getRow());

        return writer.getRow();
    }

    @Test
    public void testSource() {
        TestFuryTestStore.sourcePbInputs.clear();
        TestFuryTestStore.sourcePbInputs.add(testBinaryRow().toBytes());

        env().setParallelism(1);
        TableDescriptor descriptor = TableDescriptor.forConnector("fury-test-connector")
                .format("fury")
                .schema(org.apache.flink.table.api.Schema.newBuilder().fromResolvedSchema(schema()).build())
                .option("fury.ignore-parse-errors", "false")
                .build();
        tEnv().createTable("source_table", descriptor);

        TableResult result = tEnv().executeSql("select * from source_table");
        Row row = result.collect().next();

        assertThat(row.getField(0)).isEqualTo(true);
        assertThat(row.getField(1)).isEqualTo((byte) 1);
        assertThat(row.getField(2)).isEqualTo((short) 2);
        assertThat(row.getField(3)).isEqualTo(3);
        assertThat(row.getField(4)).isEqualTo(4L);
        assertThat(row.getField(5)).isEqualTo(5.0f);
        assertThat(row.getField(6)).isEqualTo(6.0d);
        assertThat(row.getField(7)).isEqualTo(LocalDate.ofEpochDay(7));
        assertThat(row.getField(8)).isEqualTo(Period.ofMonths(8));
        assertThat(row.getField(9)).isEqualTo(Period.ofMonths(9));
        assertThat(row.getField(10)).isEqualTo(Duration.ofMillis(10));
        assertThat(row.getField(11)).isEqualTo(Duration.ofMillis(11));
        assertThat(row.getField(12)).isEqualTo(Duration.ofMillis(12));
        assertThat(row.getField(13)).isEqualTo(Duration.ofMillis(13));
        assertThat(row.getField(14)).isEqualTo(new byte[]{1, 2, 3});
        assertThat(row.getField(15)).isEqualTo(new BigDecimal("1.23"));
        assertThat(row.getField(16)).isEqualTo(TimestampData.fromEpochMillis(16).toLocalDateTime().toLocalTime());
        assertThat(row.getField(17)).isEqualTo(TimestampData.fromEpochMillis(1723185890468L).toLocalDateTime());
        assertThat(row.getField(18)).isEqualTo(TimestampData.fromEpochMillis(1723185890468L).toInstant());
        assertThat(row.getField(19)).isEqualTo(Collections.singletonMap("hello", "world"));
        assertThat(row.getField(20)).isEqualTo(new String[]{"hello", "world"});
        assertThat(row.getField(21)).isEqualTo(Row.of("hello", 123L));
    }

    @Test
    public void testSink() throws ExecutionException, InterruptedException {
        TestFuryTestStore.sourcePbInputs.clear();
        TestFuryTestStore.sourcePbInputs.add(testBinaryRow().toBytes());
        TestFuryTestStore.sinkResults.clear();

        String dataId = TestValuesTableFactory.registerData(Collections.singletonList(testRow()));
        TableDescriptor valueDescriptor = TableDescriptor.forConnector("values")
                .schema(org.apache.flink.table.api.Schema.newBuilder().fromResolvedSchema(schema()).build())
                .option("data-id", dataId)
                .option("bounded", "true")
                .option("disable-lookup", "true")
                .build();
        tEnv().createTable("source_table", valueDescriptor);

        TableDescriptor furyDescriptor = TableDescriptor.forConnector("fury-test-connector")
                .format("fury")
                .schema(org.apache.flink.table.api.Schema.newBuilder().fromResolvedSchema(schema()).build())
                .option("fury.ignore-parse-errors", "false")
                .build();
        tEnv().createTable("sink_table", furyDescriptor);
        tEnv().executeSql("insert into sink_table select * from source_table").await();

        byte[] outputs = TestFuryTestStore.sinkResults.get(0);
        BinaryRow binaryRow = new BinaryRow(
                new Schema(
                        FurySchemaConverter.convertToSchema(
                                schema().toPhysicalRowDataType().getLogicalType()).getChildren()));
        binaryRow.pointTo(MemoryBuffer.fromByteArray(outputs), 0, outputs.length);

        assertThat(binaryRow.getBoolean(0)).isEqualTo(true);
    }
}
