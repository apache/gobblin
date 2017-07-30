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

package gobblin.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonNode;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/***
 * This class provides methods to flatten an Avro Schema to make it more optimal for ORC
 * (Hive does not support predicate pushdown for ORC with nested fields: ETL-7214)
 *
 * The behavior of Avro Schema un-nesting is listed below:
 *
 * 1. Record within Record (and so on recursively) are flattened into the parent Record
 * Record R1 {
 *   fields: {[
 *      {
 *        Record R2 {
 *          fields: {[
 *              {
 *                Record R3 {
 *                  fields: {[
 *                      {
 *                        String S2
 *                      }
 *                  ]}
 *                }, {
 *                  String S3
 *                }
 *              }
 *
 *          ]}
 *        }
 *      }, {
 *        String S1
 *      }
 *   ]}
 * }
 * will be flattened to:
 * Record R1 {
 *   fields: {[
 *      {
 *        String S1
 *      }, {
 *        String S2
 *      }, {
 *        String S3
 *      }
 *   ]}
 * }
 *
 * 2. All fields un-nested from a Record within an Option (ie. Union of the type [null, Record] or [Record, null])
 * within a Record are moved to parent Record as a list of Option fields
 * Record R1 {
 *   fields : {[
 *      {
 *        Union : [
 *          null,
 *          Record R2 {
 *            fields : {[
 *                {
 *                  String S1
 *                }, {
 *                  String S2
 *                }
 *            ]}
 *          }
 *      }
 *   ]}
 * }
 * will be flattened to:
 * Record R1 {
 *   fields : {[
 *      {
 *        Union : [ null, String S1]
 *      }, {
 *        Union : [ null, String S2]
 *      }
 *   ]}
 * }
 *
 * 3. Array or Map will not be un-nested, however Records within it will be un-nested as described above
 *
 * 4. All un-nested fields are decorated with a new property "flatten_source" which is a dot separated string
 * concatenation of parent fields name, similarly un-nested fields are renamed to double-underscore string
 * concatenation of parent fields name
 *
 * 5. Primitive Types are not un-nested
 */
public class AvroFlattener {

  private static final Logger LOG = Logger.getLogger(AvroFlattener.class);

  private static final String FLATTENED_NAME_JOINER = "__";
  private static final String FLATTENED_SOURCE_JOINER = ".";
  private static final String FLATTENED_SOURCE_KEY = "flatten_source";

  private String flattenedNameJoiner;
  private String flattenedSourceJoiner;

  /***
   * Flatten the Schema to un-nest recursive Records (to make it optimal for ORC)
   * @param schema Avro Schema to flatten
   * @param flattenComplexTypes Flatten complex types recursively other than Record and Option
   * @return Flattened Avro Schema
   */
  public Schema flatten(Schema schema, boolean flattenComplexTypes) {
    Preconditions.checkNotNull(schema);

    // To help make it configurable later
    this.flattenedNameJoiner = FLATTENED_NAME_JOINER;
    this.flattenedSourceJoiner = FLATTENED_SOURCE_JOINER;

    Schema flattenedSchema = flatten(schema, false, flattenComplexTypes);

    LOG.debug("Original Schema : " + schema);
    LOG.debug("Flattened Schema: " + flattenedSchema);

    return flattenedSchema;
  }

  /***
   * Flatten the Schema to un-nest recursive Records (to make it optimal for ORC)
   * @param schema Schema to flatten
   * @param shouldPopulateLineage is set to true if the field is going to be flattened and moved up the hierarchy -
   *                              so that lineage information can be tagged to it; which happens when there is a
   *                              Record within a Record OR Record within Option within Record and so on,
   *                              however not when there is a Record within Map or Array
   * @param flattenComplexTypes Flatten complex types recursively other than Record and Option
   * @return Flattened Avro Schema
   */
  private Schema flatten(Schema schema, boolean shouldPopulateLineage, boolean flattenComplexTypes) {
    Schema flattenedSchema;

    // Process all Schema Types
    // (Primitives are simply cloned)
    switch (schema.getType()) {
      case ARRAY:
        // Array might be an array of recursive Records, flatten them
        if (flattenComplexTypes) {
          flattenedSchema = Schema.createArray(flatten(schema.getElementType(), false));
        } else {
          flattenedSchema = Schema.createArray(schema.getElementType());
        }
        break;
      case BOOLEAN:
        flattenedSchema = Schema.create(schema.getType());
        break;
      case BYTES:
        flattenedSchema = Schema.create(schema.getType());
        break;
      case DOUBLE:
        flattenedSchema = Schema.create(schema.getType());
        break;
      case ENUM:
        flattenedSchema =
            Schema.createEnum(schema.getName(), schema.getDoc(), schema.getNamespace(), schema.getEnumSymbols());
        break;
      case FIXED:
        flattenedSchema =
            Schema.createFixed(schema.getName(), schema.getDoc(), schema.getNamespace(), schema.getFixedSize());
        break;
      case FLOAT:
        flattenedSchema = Schema.create(schema.getType());
        break;
      case INT:
        flattenedSchema = Schema.create(schema.getType());
        break;
      case LONG:
        flattenedSchema = Schema.create(schema.getType());
        break;
      case MAP:
        if (flattenComplexTypes) {
          flattenedSchema = Schema.createMap(flatten(schema.getValueType(), false));
        } else {
          flattenedSchema = Schema.createMap(schema.getValueType());
        }
        break;
      case NULL:
        flattenedSchema = Schema.create(schema.getType());
        break;
      case RECORD:
        flattenedSchema = flattenRecord(schema, shouldPopulateLineage, flattenComplexTypes);
        break;
      case STRING:
        flattenedSchema = Schema.create(schema.getType());
        break;
      case UNION:
        flattenedSchema = flattenUnion(schema, shouldPopulateLineage, flattenComplexTypes);
        break;
      default:
        String exceptionMessage = String.format("Schema flattening failed for \"%s\" ", schema);
        LOG.error(exceptionMessage);

        throw new AvroRuntimeException(exceptionMessage);
    }

    // Copy schema metadata
    copyProperties(schema, flattenedSchema);

    return flattenedSchema;
  }

  /***
   * Flatten Record schema
   * @param schema Record Schema to flatten
   * @param shouldPopulateLineage If lineage information should be tagged in the field, this is true when we are
   *                              un-nesting fields
   * @param flattenComplexTypes Flatten complex types recursively other than Record and Option
   * @return Flattened Record Schema
   */
  private Schema flattenRecord(Schema schema, boolean shouldPopulateLineage, boolean flattenComplexTypes) {
    Preconditions.checkNotNull(schema);
    Preconditions.checkArgument(Schema.Type.RECORD.equals(schema.getType()));

    Schema flattenedSchema;

    List<Schema.Field> flattenedFields = new ArrayList<>();
    if (schema.getFields().size() > 0) {
      for (Schema.Field oldField : schema.getFields()) {
        List<Schema.Field> newFields = flattenField(oldField, ImmutableList.<String>of(),
            shouldPopulateLineage, flattenComplexTypes, Optional.<Schema>absent());
        if (null != newFields && newFields.size() > 0) {
          flattenedFields.addAll(newFields);
        }
      }
    }

    flattenedSchema = Schema.createRecord(schema.getName(), schema.getDoc(), schema.getNamespace(),
        schema.isError());
    flattenedSchema.setFields(flattenedFields);

    return flattenedSchema;
  }

  /***
   * Flatten Union Schema
   * @param schema Union Schema to flatten
   * @param shouldPopulateLineage If lineage information should be tagged in the field, this is true when we are
   *                              un-nesting fields
   * @param flattenComplexTypes Flatten complex types recursively other than Record and Option
   * @return Flattened Union Schema
   */
  private Schema flattenUnion(Schema schema, boolean shouldPopulateLineage, boolean flattenComplexTypes) {
    Preconditions.checkNotNull(schema);
    Preconditions.checkArgument(Schema.Type.UNION.equals(schema.getType()));

    Schema flattenedSchema;

    List<Schema> flattenedUnionMembers = new ArrayList<>();
    if (null != schema.getTypes() && schema.getTypes().size() > 0) {
      for (Schema oldUnionMember : schema.getTypes()) {
        if (flattenComplexTypes) {
          // It's member might still recursively contain records
          flattenedUnionMembers.add(flatten(oldUnionMember, shouldPopulateLineage, flattenComplexTypes));
        } else {
          flattenedUnionMembers.add(oldUnionMember);
        }
      }
    }
    flattenedSchema = Schema.createUnion(flattenedUnionMembers);

    return flattenedSchema;
  }

  /***
   * Flatten Record field, and compute a list of flattened fields
   *
   * Note: Lineage represents the source path from root for the flattened field. For. eg. If the original schema is:
   * {
   *    "type" : "record",
   *    "name" : "parentRecordName",
   *    "fields" : [ {
   *      "name" : "parentFieldRecord",
   *      "type" : {
   *        "type" : "record",
   *        "name" : "nestedRecordName",
   *        "fields" : [ {
   *            "name" : "nestedFieldString",
   *            "type" : "string"
   *          }, {
   *            "name" : "nestedFieldInt",
   *            "type" : "int"
   *          } ]
   *       }
   *     }]
   * }
   * The expected output schema is:
   * {
   *    "type" : "record",
   *    "name" : "parentRecordName",
   *    "fields" : [ {
   *      "name" : "parentFieldRecord__nestedFieldString",
   *      "type" : "string",
   *      "flatten_source" : "parentFieldRecord.nestedFieldString"
   *    }, {
   *      "name" : "parentFieldRecord__nestedFieldInt",
   *      "type" : "int",
   *      "flatten_source" : "parentFieldRecord.nestedFieldInt"
   *    }, {
   *      "name" : "parentFieldInt",
   *      "type" : "int"
   *    } ]
   * }
   * Here, 'flatten_source' and field 'name' has also been modified to represent their origination from nested schema
   * lineage helps to determine that
   *
   * @param f Field to flatten
   * @param parentLineage Parent's lineage represented as a List of Strings
   * @param shouldPopulateLineage If lineage information should be tagged in the field, this is true when we are
   *                              un-nesting fields
   * @param flattenComplexTypes Flatten complex types recursively other than Record and Option
   * @param shouldWrapInOption If the field should be wrapped as an OPTION, if we un-nest fields within an OPTION
   *                           we make all the unnested fields as OPTIONs
   * @return List of flattened Record fields
   */
  private List<Schema.Field> flattenField(Schema.Field f, ImmutableList<String> parentLineage,
      boolean shouldPopulateLineage, boolean flattenComplexTypes, Optional<Schema> shouldWrapInOption) {
    Preconditions.checkNotNull(f);
    Preconditions.checkNotNull(f.schema());
    Preconditions.checkNotNull(f.name());

    List<Schema.Field> flattenedFields = new ArrayList<>();
    ImmutableList<String> lineage = ImmutableList.<String>builder()
        .addAll(parentLineage.iterator()).add(f.name()).build();

    // If field.Type = RECORD, un-nest its fields and return them
    if (Schema.Type.RECORD.equals(f.schema().getType())) {
      if (null != f.schema().getFields() && f.schema().getFields().size() > 0) {
        for (Schema.Field field : f.schema().getFields()) {
          flattenedFields.addAll(flattenField(field, lineage, true, flattenComplexTypes, Optional.<Schema>absent()));
        }
      }
    }
    // If field.Type = OPTION, un-nest its fields and return them
    else {
      Optional<Schema> optionalRecord = isOfOptionType(f.schema());
      if (optionalRecord.isPresent()) {
        Schema record = optionalRecord.get();
        if (record.getFields().size() > 0) {
          for (Schema.Field field : record.getFields()) {
            flattenedFields.addAll(flattenField(field, lineage, true, flattenComplexTypes, Optional.of(f.schema())));
          }
        }
      }
      // If field.Type = any-other, copy and return it
      else {
        // Compute name and source using lineage
        String flattenName = f.name();
        String flattenSource = StringUtils.EMPTY;
        if (shouldPopulateLineage) {
          flattenName = StringUtils.join(lineage, flattenedNameJoiner);
          flattenSource = StringUtils.join(lineage, flattenedSourceJoiner);
        }
        // Copy field
        Schema flattenedFieldSchema = flatten(f.schema(), shouldPopulateLineage, flattenComplexTypes);
        if (shouldWrapInOption.isPresent()) {
          boolean isNullFirstMember = Schema.Type.NULL.equals(shouldWrapInOption.get().getTypes().get(0).getType());
          // If already Union, just copy it instead of wrapping (Union within Union is not supported)
          if (Schema.Type.UNION.equals(flattenedFieldSchema.getType())) {
            List<Schema> newUnionMembers = new ArrayList<>();
            if (isNullFirstMember) {
              newUnionMembers.add(Schema.create(Schema.Type.NULL));
            }
            for (Schema type : flattenedFieldSchema.getTypes()) {
              if (Schema.Type.NULL.equals(type.getType())) {
                continue;
              }
              newUnionMembers.add(type);
            }
            if (!isNullFirstMember) {
              newUnionMembers.add(Schema.create(Schema.Type.NULL));
            }

            flattenedFieldSchema = Schema.createUnion(newUnionMembers);
          }
          // Wrap the Union, since parent Union is an option
          else {
            if (isNullFirstMember) {
              flattenedFieldSchema =
                  Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), flattenedFieldSchema));
            } else {
              flattenedFieldSchema =
                  Schema.createUnion(Arrays.asList(flattenedFieldSchema, Schema.create(Schema.Type.NULL)));
            }
          }
        }
        Schema.Field field = new Schema.Field(flattenName, flattenedFieldSchema, f.doc(), f.defaultValue(), f.order());

        if (StringUtils.isNotBlank(flattenSource)) {
          field.addProp(FLATTENED_SOURCE_KEY, flattenSource);
        }
        for (Map.Entry<String, JsonNode> entry : f.getJsonProps().entrySet()) {
          field.addProp(entry.getKey(), entry.getValue());
        }
        flattenedFields.add(field);
      }
    }

    return flattenedFields;
  }

  /***
   * Check if the Avro Schema is of type OPTION
   * ie. [null, RECORD] or [RECORD, null]
   * @param schema Avro Schema to check
   * @return Optional Avro Record if schema is of type OPTION
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

      // One member should be of type NULL and other of type RECORD
      if (Schema.Type.NULL.equals(first.getType()) && Schema.Type.RECORD.equals(second.getType())) {
        return Optional.of(second);
      } else if (Schema.Type.RECORD.equals(first.getType()) && Schema.Type.NULL.equals(second.getType())) {
        return Optional.of(first);
      }
    }

    return Optional.<Schema>absent();
  }

  /***
   * Copy properties from old Avro Schema to new Avro Schema
   * @param oldSchema Old Avro Schema to copy properties from
   * @param newSchema New Avro Schema to copy properties to
   */
  private static void copyProperties(Schema oldSchema, Schema newSchema) {
    Preconditions.checkNotNull(oldSchema);
    Preconditions.checkNotNull(newSchema);

    Map<String, JsonNode> props = oldSchema.getJsonProps();
    copyProperties(props, newSchema);
  }

  /***
   * Copy properties to an Avro Schema
   * @param props Properties to copy to Avro Schema
   * @param schema Avro Schema to copy properties to
   */
  private static void copyProperties(Map<String, JsonNode> props, Schema schema) {
    Preconditions.checkNotNull(schema);

    // (if null, don't copy but do not throw exception)
    if (null != props) {
      for (Map.Entry<String, JsonNode> prop : props.entrySet()) {
        schema.addProp(prop.getKey(), prop.getValue());
      }
    }
  }
}
