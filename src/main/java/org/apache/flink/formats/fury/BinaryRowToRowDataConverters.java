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
import org.apache.flink.table.data.*;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.types.logical.*;
import org.apache.fury.format.row.ArrayData;
import org.apache.fury.format.row.binary.BinaryArray;
import org.apache.fury.format.row.binary.BinaryMap;
import org.apache.fury.format.row.binary.BinaryRow;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BinaryRowToRowDataConverters implements Serializable {

    private static final long serialVersionUID = 1L;

    public interface BinaryRowToRowDataConverter extends Serializable {
        Object convert(Object input);
    }

    public static BinaryRowToRowDataConverter createConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case NULL:
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case DATE:
            case INTERVAL_YEAR_MONTH:
            case TIME_WITHOUT_TIME_ZONE:
            case BIGINT:
            case INTERVAL_DAY_TIME:
            case FLOAT:
            case DOUBLE:
            case BINARY:
            case VARBINARY:
                return input -> input;
            case CHAR:
            case VARCHAR:
                return input -> BinaryStringData.fromString(input.toString());
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return input -> TimestampData.fromEpochMillis((long) input);
            case DECIMAL:
                DecimalType decimalType = (DecimalType) type;
                return input ->
                        DecimalData.fromBigDecimal(
                                (BigDecimal) input,
                                decimalType.getPrecision(),
                                decimalType.getScale());
            case ARRAY:
                return createArrayConverter((ArrayType) type);
            case ROW:
                return createRowConverter((RowType) type);
            case MAP:
                return createMapConverter((MapType) type);
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }

    private static BinaryRowToRowDataConverter createRowConverter(RowType rowType) {
        final BinaryRowToRowDataConverter[] fieldConverters =
                rowType.getChildren().stream()
                        .map(BinaryRowToRowDataConverters::createConverter)
                        .toArray(BinaryRowToRowDataConverter[]::new);
        return input -> {
            BinaryRow inputRow = (BinaryRow) input;
            List<Field> fields = inputRow.getSchema().getFields();
            GenericRowData result = new GenericRowData(inputRow.numFields());
            for (int i = 0; i < result.getArity(); i++) {
                if (inputRow.isNullAt(i)) {
                    result.setField(i, null);
                } else {
                    result.setField(i, fieldConverters[i].convert(inputRow.get(i, fields.get(i))));
                }
            }
            return result;
        };
    }

    private static BinaryRowToRowDataConverter createMapConverter(MapType mapType) {
        LogicalType keyType = mapType.getKeyType();
        LogicalType valueType = mapType.getValueType();

        Field keyField = FurySchemaConverter.convertToSchema(keyType);
        Field valueField = FurySchemaConverter.convertToSchema(valueType);

        final BinaryRowToRowDataConverter keyConverter = createConverter(keyType);
        final BinaryRowToRowDataConverter valueConverter = createConverter(valueType);

        return input -> {
            BinaryMap binaryMap = (BinaryMap) input;
            BinaryArray keyArray = binaryMap.keyArray();
            BinaryArray valueArray = binaryMap.valueArray();

            int elements = keyArray.numElements();
            Map<Object, Object> map = new HashMap<>(elements);

            for (int i = 0; i < elements; i++) {
                Object key = keyConverter.convert(keyArray.get(i, keyField));
                Object value = valueConverter.convert(valueArray.get(i, valueField));
                map.put(key, value);
            }
            return new GenericMapData(map);
        };
    }

    private static BinaryRowToRowDataConverter createArrayConverter(ArrayType arrayType) {
        LogicalType elementType = arrayType.getElementType();
        Field field = FurySchemaConverter.convertToSchema(elementType);

        final BinaryRowToRowDataConverter converter = createConverter(arrayType.getElementType());

        return input -> {
            ArrayData arrayData = (ArrayData) input;
            int size = arrayData.numElements();
            Object[] array = new Object[size];
            for (int i = 0; i < size; i++) {
                array[i] = converter.convert(arrayData.get(i, field));
            }

            return new GenericArrayData(array);
        };
    }
}
