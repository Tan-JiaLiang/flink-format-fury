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
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.*;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.fury.format.row.binary.BinaryRow;
import org.apache.fury.format.row.binary.writer.BinaryRowWriter;
import org.apache.fury.memory.MemoryBuffer;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

public class RowDataConverterTest {

    @Test
    public void testConverter() {
        ResolvedSchema resolvedSchema = ResolvedSchema.of(
                Column.physical("f_null", DataTypes.NULL()),
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

        GenericRowData input = GenericRowData.of(
                null,
                true,
                (byte) 1,
                (short) 2,
                3,
                4L,
                5.0f,
                6.0d,
                7,    // date
                8,    // interval year
                9,  // interval month
                10L, // interval day
                11L,    // interval hour
                12L,  // interval minute
                13L, // interval second
                new byte[]{1, 2, 3}, // varbinary
                DecimalData.fromBigDecimal(new BigDecimal("1.23"), 3, 2), // decimal
                16,   // time
                TimestampData.fromEpochMillis(1723185890468L), // timestamp
                TimestampData.fromEpochMillis(1723185890468L), // timestamp_ltz
                new GenericMapData(Collections.singletonMap(StringData.fromString("hello"), StringData.fromString("world"))),
                new GenericArrayData(new Object[]{StringData.fromString("hello"), StringData.fromString("world")}),
                GenericRowData.of(StringData.fromString("hello"), 123L)
        );

        LogicalType logicalType = resolvedSchema.toPhysicalRowDataType().getLogicalType();

        RowDataToBinaryRowConverters.RowDataToBinaryRowFiller filler =
                RowDataToBinaryRowConverters.createFiller(logicalType);
        BinaryRowWriter writer = new BinaryRowWriter(
                new Schema(Collections.singletonList(FurySchemaConverter.convertToSchema(logicalType))));
        writer.reset();
        filler.fill(writer, 0, input);

        BinaryRow row = writer.getRow().getStruct(0);
        assertThat(row.isNullAt(0)).isTrue();
        assertThat(row.getBoolean(1)).isTrue();
        assertThat(row.getByte(2)).isEqualTo((byte) 1);
        assertThat(row.getInt16(3)).isEqualTo((short) 2);
        assertThat(row.getInt32(4)).isEqualTo(3);
        assertThat(row.getInt64(5)).isEqualTo(4L);
        assertThat(row.getFloat32(6)).isEqualTo(5.0f);
        assertThat(row.getFloat64(7)).isEqualTo(6.0d);
        assertThat(row.getInt32(8)).isEqualTo(7);
        assertThat(row.getInt32(9)).isEqualTo(8);
        assertThat(row.getInt32(10)).isEqualTo(9);
        assertThat(row.getInt64(11)).isEqualTo(10L);
        assertThat(row.getInt64(12)).isEqualTo(11L);
        assertThat(row.getInt64(13)).isEqualTo(12L);
        assertThat(row.getInt64(14)).isEqualTo(13L);
        assertThat(row.getBinary(15)).isEqualTo(new byte[]{1, 2, 3});
        assertThat(row.getDecimal(16)).isEqualTo(new BigDecimal("1.23"));
        assertThat(row.getInt32(17)).isEqualTo(16);
        assertThat(row.getInt64(18)).isEqualTo(1723185890468L);
        assertThat(row.getInt64(19)).isEqualTo(1723185890468L);
        assertThat(row.getMap(20).keyArray().getString(0)).isEqualTo("hello");
        assertThat(row.getMap(20).valueArray().getString(0)).isEqualTo("world");
        assertThat(row.getArray(21).getString(0)).isEqualTo("hello");
        assertThat(row.getArray(21).getString(1)).isEqualTo("world");
        assertThat(row.getStruct(22).getString(0)).isEqualTo("hello");
        assertThat(row.getStruct(22).getInt64(1)).isEqualTo(123L);

        BinaryRowToRowDataConverters.BinaryRowToRowDataConverter converter =
                BinaryRowToRowDataConverters.createConverter(logicalType);

        BinaryRow outputRow = new BinaryRow(
                new Schema(FurySchemaConverter.convertToSchema(logicalType).getChildren()));
        outputRow.pointTo(MemoryBuffer.fromByteArray(row.toBytes()), 0, row.getSizeInBytes());
        RowData output = (RowData) converter.convert(outputRow);
        assertThat(output.isNullAt(0)).isTrue();
        assertThat(output.getBoolean(1)).isTrue();
        assertThat(output.getByte(2)).isEqualTo((byte) 1);
        assertThat(output.getShort(3)).isEqualTo((short) 2);
        assertThat(output.getInt(4)).isEqualTo(3);
        assertThat(output.getLong(5)).isEqualTo(4L);
        assertThat(output.getFloat(6)).isEqualTo(5.0f);
        assertThat(output.getDouble(7)).isEqualTo(6.0d);
        assertThat(output.getInt(8)).isEqualTo(7);
        assertThat(output.getInt(9)).isEqualTo(8);
        assertThat(output.getInt(10)).isEqualTo(9);
        assertThat(output.getLong(11)).isEqualTo(10L);
        assertThat(output.getLong(12)).isEqualTo(11L);
        assertThat(output.getLong(13)).isEqualTo(12L);
        assertThat(output.getLong(14)).isEqualTo(13L);
        assertThat(output.getBinary(15)).isEqualTo(new byte[]{1, 2, 3});
        assertThat(output.getDecimal(16, 3, 2)).isEqualTo(DecimalData.fromBigDecimal(new BigDecimal("1.23"), 3, 2));
        assertThat(output.getInt(17)).isEqualTo(16);
        assertThat(output.getTimestamp(18, 3)).isEqualTo(TimestampData.fromEpochMillis(1723185890468L));
        assertThat(output.getTimestamp(19, 3)).isEqualTo(TimestampData.fromEpochMillis(1723185890468L));
        assertThat(output.getMap(20).keyArray().getString(0)).isEqualTo(StringData.fromString("hello"));
        assertThat(output.getMap(20).valueArray().getString(0)).isEqualTo(StringData.fromString("world"));
        assertThat(output.getArray(21).getString(0)).isEqualTo(StringData.fromString("hello"));
        assertThat(output.getArray(21).getString(1)).isEqualTo(StringData.fromString("world"));
        assertThat(output.getRow(22, 2).getString(0)).isEqualTo(StringData.fromString("hello"));
        assertThat(output.getRow(22, 2).getLong(1)).isEqualTo(123L);
    }
}
