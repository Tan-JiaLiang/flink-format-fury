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

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.flink.table.types.logical.*;
import org.apache.fury.format.type.DataTypes;

import java.util.ArrayList;
import java.util.List;

public class FurySchemaConverter {

    private FurySchemaConverter() {
    }

    public static Field convertToSchema(LogicalType logicalType) {
        return convertToSchema("", logicalType);
    }

    public static Field convertToSchema(String name, LogicalType logicalType) {
        int precision;
        boolean nullable = logicalType.isNullable();
        switch (logicalType.getTypeRoot()) {
            case NULL:
                return DataTypes.field(name, nullable, ArrowType.Null.INSTANCE);
            case BOOLEAN:
                return DataTypes.field(name, nullable, DataTypes.bool());
            case TINYINT:
                return DataTypes.field(name, nullable, DataTypes.int8());
            case SMALLINT:
                return DataTypes.field(name, nullable, DataTypes.int16());
            case DATE:
            case INTERVAL_YEAR_MONTH:
            case TIME_WITHOUT_TIME_ZONE:
            case INTEGER:
                return DataTypes.field(name, nullable, DataTypes.int32());
            case INTERVAL_DAY_TIME:
            case BIGINT:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return DataTypes.field(name, nullable, DataTypes.int64());
            case FLOAT:
                return DataTypes.field(name, nullable, DataTypes.float32());
            case DOUBLE:
                return DataTypes.field(name, nullable, DataTypes.float64());
            case CHAR:
            case VARCHAR:
                return DataTypes.field(name, nullable, DataTypes.utf8());
            case BINARY:
            case VARBINARY:
                return DataTypes.field(name, nullable, DataTypes.binary());
            case DECIMAL:
                DecimalType decimalType = (DecimalType) logicalType;
                precision = decimalType.getPrecision();
                int scale = decimalType.getScale();
                return DataTypes.field(name, nullable, DataTypes.decimal(precision, scale));
            case MAP:
                MapType mapType = (MapType) logicalType;
                Field keyField = convertToSchema(mapType.getKeyType());
                Field valueField = convertToSchema(mapType.getValueType());
                return DataTypes.mapField(name, keyField, valueField);
            case ARRAY:
                ArrayType arrayType = (ArrayType) logicalType;
                Field field = convertToSchema(arrayType.getElementType());
                return DataTypes.arrayField(name, field);
            case ROW:
                RowType rowType = (RowType) logicalType;
                List<String> fieldNames = rowType.getFieldNames();
                List<Field> fields = new ArrayList<>();
                for (int i = 0; i < rowType.getFieldCount(); i++) {
                    String fieldName = fieldNames.get(i);
                    LogicalType fieldType = rowType.getTypeAt(i);
                    fields.add(convertToSchema(fieldName, fieldType));
                }
                return DataTypes.structField(name, nullable, fields);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return DataTypes.field(name, nullable, DataTypes.timestamp());
            default:
                throw new UnsupportedOperationException(
                        "Unsupported to derive Schema for type: " + logicalType);
        }
    }
}
