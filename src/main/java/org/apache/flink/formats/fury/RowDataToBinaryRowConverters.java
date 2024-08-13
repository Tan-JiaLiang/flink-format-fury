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
import org.apache.flink.table.data.*;
import org.apache.flink.table.types.logical.*;
import org.apache.fury.format.row.binary.BinaryMap;
import org.apache.fury.format.row.binary.writer.BinaryArrayWriter;
import org.apache.fury.format.row.binary.writer.BinaryRowWriter;
import org.apache.fury.format.row.binary.writer.BinaryWriter;
import org.apache.fury.format.type.DataTypes;

import java.io.Serializable;

public class RowDataToBinaryRowConverters implements Serializable {

    private static final long serialVersionUID = 1L;

    public interface RowDataToBinaryRowFiller extends Serializable {
        void fill(BinaryWriter writer, int index, Object input);
    }

    public static RowDataToBinaryRowFiller createFiller(LogicalType type) {
        switch (type.getTypeRoot()) {
            case NULL:
                return (writer, index, input) -> writer.setNullAt(index);
            case BOOLEAN:
                return (writer, index, input) -> writer.write(index, (boolean) input);
            case TINYINT:
                return (writer, index, input) -> writer.write(index, (byte) input);
            case SMALLINT:
                return (writer, index, input) -> writer.write(index, (short) input);
            case DATE:
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
            case TIME_WITHOUT_TIME_ZONE:
                return (writer, index, input) -> writer.write(index, (int) input);
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return (writer, index, input) -> writer.write(index, (long) input);
            case FLOAT:
                return (writer, index, input) -> writer.write(index, (float) input);
            case DOUBLE:
                return (writer, index, input) -> writer.write(index, (double) input);
            case BINARY:
            case VARBINARY:
                return (writer, index, input) -> writer.write(index, (byte[]) input);
            case CHAR:
            case VARCHAR:
                return (writer, index, input) -> writer.write(index, input.toString());
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return (writer, index, input) -> writer.write(index, ((TimestampData) input).toInstant().toEpochMilli());
            case DECIMAL:
                return (writer, index, input) -> writer.write(index, ((DecimalData) input).toBigDecimal());
            case ARRAY:
                return createArrayFiler((ArrayType) type);
            case ROW:
                return createRowFiller((RowType) type);
            case MAP:
                return createMapFiller((MapType) type);
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }

    private static RowDataToBinaryRowFiller createMapFiller(MapType mapType) {
        LogicalType keyType = mapType.getKeyType();
        LogicalType valueType = mapType.getValueType();

        final ArrayData.ElementGetter keyGetter = ArrayData.createElementGetter(keyType);
        final ArrayData.ElementGetter valueGetter = ArrayData.createElementGetter(valueType);
        RowDataToBinaryRowFiller keyFiller = createFiller(keyType);
        RowDataToBinaryRowFiller valueFiller = createFiller(valueType);

        Field field = FurySchemaConverter.convertToSchema(mapType);
        Field keyField = DataTypes.arrayField(FurySchemaConverter.convertToSchema(keyType));
        Field valueField = DataTypes.arrayField(FurySchemaConverter.convertToSchema(valueType));

        return (writer, index, input) -> {
            MapData mapData = (MapData) input;

            ArrayData keyArrayData = mapData.keyArray();
            BinaryArrayWriter keyWriter = new BinaryArrayWriter(keyField);
            keyWriter.reset(keyArrayData.size());
            for (int i = 0; i < keyArrayData.size(); i++) {
                Object element = keyGetter.getElementOrNull(keyArrayData, i);
                if (element == null) {
                    keyWriter.setNullAt(i);
                } else {
                    keyFiller.fill(keyWriter, i, element);
                }
            }

            ArrayData valueArrayData = mapData.valueArray();
            BinaryArrayWriter valueWriter = new BinaryArrayWriter(valueField);
            valueWriter.reset(valueArrayData.size());
            for (int i = 0; i < valueArrayData.size(); i++) {
                Object element = valueGetter.getElementOrNull(valueArrayData, i);
                if (element == null) {
                    valueWriter.setNullAt(i);
                } else {
                    valueFiller.fill(valueWriter, i, element);
                }
            }

            writer.write(index, new BinaryMap(keyWriter.toArray(), valueWriter.toArray(), field));
        };
    }

    private static RowDataToBinaryRowFiller createArrayFiler(ArrayType arrayType) {
        LogicalType elementType = arrayType.getElementType();
        final ArrayData.ElementGetter elementGetter = ArrayData.createElementGetter(elementType);
        final RowDataToBinaryRowFiller filler = createFiller(arrayType.getElementType());

        Field arrayField = FurySchemaConverter.convertToSchema(arrayType);

        return (writer, index, input) -> {
            ArrayData arrayData = (ArrayData) input;
            BinaryArrayWriter arrayWriter = new BinaryArrayWriter(arrayField);
            arrayWriter.reset(arrayData.size());
            for (int i = 0; i < arrayData.size(); i++) {
                Object element = elementGetter.getElementOrNull(arrayData, i);
                if (element == null) {
                    arrayWriter.setNullAt(i);
                } else {
                    filler.fill(arrayWriter, i, element);
                }
            }
            writer.write(index, arrayWriter.toArray());
        };
    }

    private static RowDataToBinaryRowFiller createRowFiller(RowType rowType) {
        final RowDataToBinaryRowFiller[] fillers =
                rowType.getChildren().stream()
                        .map(RowDataToBinaryRowConverters::createFiller)
                        .toArray(RowDataToBinaryRowFiller[]::new);
        final LogicalType[] fieldTypes =
                rowType.getFields().stream()
                        .map(RowType.RowField::getType)
                        .toArray(LogicalType[]::new);
        final RowData.FieldGetter[] fieldGetters = new RowData.FieldGetter[fieldTypes.length];
        for (int i = 0; i < fieldTypes.length; i++) {
            LogicalType fieldType = fieldTypes[i];
            if (LogicalTypeRoot.NULL.equals(fieldType.getTypeRoot())) {
                fieldGetters[i] = (input) -> null;
            } else {
                fieldGetters[i] = RowData.createFieldGetter(fieldType, i);
            }
        }
        final int length = rowType.getFieldCount();

        Field field = FurySchemaConverter.convertToSchema(rowType);
        Schema schema = new Schema(field.getChildren());

        return (writer, index, input) -> {
            final RowData row = (RowData) input;
            BinaryRowWriter rowWriter = new BinaryRowWriter(schema);
            rowWriter.reset();
            for (int i = 0; i < length; i++) {
                Object fieldOrNull = fieldGetters[i].getFieldOrNull(row);
                if (fieldOrNull == null) {
                    rowWriter.setNullAt(i);
                } else {
                    fillers[i].fill(rowWriter, i, fieldOrNull);
                }
            }
            writer.write(index, rowWriter.getRow());
        };
    }
}
