/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gobblin.data.management.conversion.hive.utils;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import java.util.List;
import java.util.Map;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.serde2.avro.AvroSerDe;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeException;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.util.HiveAvroTypeConstants;


/**
 * Utility class that deals with type conversion between Avro and Hive.
 */
@Slf4j
public class AvroHiveTypeUtils {
  private AvroHiveTypeUtils() {

  }

  public static String generateAvroToHiveColumnMapping(Schema schema, Optional<Map<String, String>> hiveColumns,
      boolean topLevel, String datasetName) {
    if (topLevel && !schema.getType().equals(Schema.Type.RECORD)) {
      throw new IllegalArgumentException(String
          .format("Schema for table must be of type RECORD. Received type: %s for dataset %s", schema.getType(),
              datasetName));
    }

    StringBuilder columns = new StringBuilder();
    boolean isFirst;
    switch (schema.getType()) {
      case RECORD:
        isFirst = true;
        if (topLevel) {
          for (Schema.Field field : schema.getFields()) {
            if (isFirst) {
              isFirst = false;
            } else {
              columns.append(", \n");
            }
            String type = generateAvroToHiveColumnMapping(field.schema(), hiveColumns, false, datasetName);
            if (hiveColumns.isPresent()) {
              hiveColumns.get().put(field.name(), type);
            }
            String flattenSource = field.getProp("flatten_source");
            if (StringUtils.isBlank(flattenSource)) {
              flattenSource = field.name();
            }
            columns
                .append(String.format("  `%s` %s COMMENT 'from flatten_source %s'", field.name(), type, flattenSource));
          }
        } else {
          columns.append(HiveAvroTypeConstants.AVRO_TO_HIVE_COLUMN_MAPPING_V_12.get(schema.getType())).append("<");
          for (Schema.Field field : schema.getFields()) {
            if (isFirst) {
              isFirst = false;
            } else {
              columns.append(",");
            }
            String type = generateAvroToHiveColumnMapping(field.schema(), hiveColumns, false, datasetName);
            columns.append("`").append(field.name()).append("`").append(":").append(type);
          }
          columns.append(">");
        }
        break;
      case UNION:
        Optional<Schema> optionalType = isOfOptionType(schema);
        if (optionalType.isPresent()) {
          Schema optionalTypeSchema = optionalType.get();
          columns.append(generateAvroToHiveColumnMapping(optionalTypeSchema, hiveColumns, false, datasetName));
        } else {
          columns.append(HiveAvroTypeConstants.AVRO_TO_HIVE_COLUMN_MAPPING_V_12.get(schema.getType())).append("<");
          isFirst = true;
          for (Schema unionMember : schema.getTypes()) {
            if (Schema.Type.NULL.equals(unionMember.getType())) {
              continue;
            }
            if (isFirst) {
              isFirst = false;
            } else {
              columns.append(",");
            }
            columns.append(generateAvroToHiveColumnMapping(unionMember, hiveColumns, false, datasetName));
          }
          columns.append(">");
        }
        break;
      case MAP:
        columns.append(HiveAvroTypeConstants.AVRO_TO_HIVE_COLUMN_MAPPING_V_12.get(schema.getType())).append("<");
        columns.append("string,")
            .append(generateAvroToHiveColumnMapping(schema.getValueType(), hiveColumns, false, datasetName));
        columns.append(">");
        break;
      case ARRAY:
        columns.append(HiveAvroTypeConstants.AVRO_TO_HIVE_COLUMN_MAPPING_V_12.get(schema.getType())).append("<");
        columns.append(generateAvroToHiveColumnMapping(schema.getElementType(), hiveColumns, false, datasetName));
        columns.append(">");
        break;
      case NULL:
        break;
      case BYTES:
      case DOUBLE:
      case ENUM:
      case FIXED:
      case FLOAT:
      case INT:
      case LONG:
      case STRING:
      case BOOLEAN:
        // Handling Avro Logical Types which should always sit in leaf-level.
        boolean isLogicalTypeSet = false;
        try {
          String hiveSpecificLogicalType = generateHiveSpecificLogicalType(schema);
          if (StringUtils.isNoneEmpty(hiveSpecificLogicalType)) {
            isLogicalTypeSet = true;
            columns.append(hiveSpecificLogicalType);
            break;
          }
        } catch (AvroSerdeException ae) {
          log.error("Failed to generate logical type string for field" + schema.getName() + " due to:", ae);
        }

        LogicalType logicalType = LogicalTypes.fromSchemaIgnoreInvalid(schema);
        if (logicalType != null) {
          switch (logicalType.getName().toLowerCase()) {
            case HiveAvroTypeConstants.DATE:
              LogicalTypes.Date dateType = (LogicalTypes.Date) logicalType;
              dateType.validate(schema);
              columns.append("date");
              isLogicalTypeSet = true;
              break;
            case HiveAvroTypeConstants.DECIMAL:
              LogicalTypes.Decimal decimalType = (LogicalTypes.Decimal) logicalType;
              decimalType.validate(schema);
              columns.append(String.format("decimal(%s, %s)", decimalType.getPrecision(), decimalType.getScale()));
              isLogicalTypeSet = true;
              break;
            case HiveAvroTypeConstants.TIME_MILLIS:
              LogicalTypes.TimeMillis timeMillsType = (LogicalTypes.TimeMillis) logicalType;
              timeMillsType.validate(schema);
              columns.append("timestamp");
              isLogicalTypeSet = true;
              break;
            default:
              log.error("Unsupported logical type" + schema.getLogicalType().getName() + ", fallback to physical type");
          }
        }

        if (!isLogicalTypeSet) {
          columns.append(HiveAvroTypeConstants.AVRO_TO_HIVE_COLUMN_MAPPING_V_12.get(schema.getType()));
        }
        break;
      default:
        String exceptionMessage =
            String.format("DDL query generation failed for \"%s\" of dataset %s", schema, datasetName);
        log.error(exceptionMessage);
        throw new AvroRuntimeException(exceptionMessage);
    }

    return columns.toString();
  }

  /**
   * Referencing org.apache.hadoop.hive.serde2.avro.SchemaToTypeInfo#generateTypeInfo(org.apache.avro.Schema) on
   * how to deal with logical types that supported by Hive but not by Avro(e.g. VARCHAR).
   *
   * If unsupported logical types found, return empty string as a result.
   * @param schema Avro schema
   * @return
   * @throws AvroSerdeException
   */
  public static String generateHiveSpecificLogicalType(Schema schema)
      throws AvroSerdeException {
    // For bytes type, it can be mapped to decimal.
    Schema.Type type = schema.getType();

    if (type == Schema.Type.STRING && AvroSerDe.VARCHAR_TYPE_NAME
        .equalsIgnoreCase(schema.getProp(AvroSerDe.AVRO_PROP_LOGICAL_TYPE))) {
      int maxLength = 0;
      try {
        maxLength = Integer.parseInt(AvroCompatibilityHelper.getSchemaPropAsJsonString(schema, 
            AvroSerDe.AVRO_PROP_MAX_LENGTH, false, false));
      } catch (Exception ex) {
        throw new AvroSerdeException("Failed to obtain maxLength value from file schema: " + schema, ex);
      }
      return String.format("varchar(%s)", maxLength);
    } else {
      return StringUtils.EMPTY;
    }
  }

  /***
   * Check if the Avro Schema is of type OPTION
   * ie. [null, TYPE] or [TYPE, null]
   * @param schema Avro Schema to check
   * @return Optional Avro Typed data if schema is of type OPTION
   */
  private static Optional<Schema> isOfOptionType(Schema schema) {
    Preconditions.checkNotNull(schema);

    // If not of type UNION, cant be an OPTION
    if (!Schema.Type.UNION.equals(schema.getType())) {
      return Optional.<Schema>absent();
    }

    // If has more than two members, can't be an OPTION
    List<Schema> types = schema.getTypes();
    if (null != types && types.size() == 2) {
      Schema first = types.get(0);
      Schema second = types.get(1);

      // One member should be of type NULL and other of non NULL type
      if (Schema.Type.NULL.equals(first.getType()) && !Schema.Type.NULL.equals(second.getType())) {
        return Optional.of(second);
      } else if (!Schema.Type.NULL.equals(first.getType()) && Schema.Type.NULL.equals(second.getType())) {
        return Optional.of(first);
      }
    }

    return Optional.<Schema>absent();
  }
}
