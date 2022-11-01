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
package org.apache.gobblin.util.orc;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.gobblin.util.AvroSchemaUtils;
import org.apache.orc.TypeDescription;


/**
 * A utility class that provides a method to convert {@link Schema} into {@link TypeDescription}.
 */
public class AvroOrcSchemaConverter {
  public static TypeDescription getOrcSchema(Schema avroSchema) {

    final Schema.Type type = avroSchema.getType();
    switch (type) {
      case NULL:
        // empty union represents null type
        final TypeDescription nullUnion = TypeDescription.createUnion();
        return nullUnion;
      case LONG:
        return TypeDescription.createLong();
      case INT:
        return TypeDescription.createInt();
      case BYTES:
      case FIXED:
        return getTypeDescriptionForBinarySchema(avroSchema);
      case ARRAY:
        return TypeDescription.createList(getOrcSchema(avroSchema.getElementType()));
      case RECORD:
        final TypeDescription recordStruct = TypeDescription.createStruct();
        for (Schema.Field field2 : avroSchema.getFields()) {
          final Schema fieldSchema = field2.schema();
          final TypeDescription fieldType = getOrcSchema(fieldSchema);
          if (fieldType != null) {
            recordStruct.addField(field2.name(), fieldType);
          } else {
            throw new IllegalStateException("Should never get a null type as fieldType.");
          }
        }
        return recordStruct;
      case MAP:
        return TypeDescription.createMap(
            // in Avro maps, keys are always strings
            TypeDescription.createString(), getOrcSchema(avroSchema.getValueType()));
      case UNION:
        final List<Schema> nonNullMembers = getNonNullMembersOfUnion(avroSchema);
        if (isNullableUnion(avroSchema, nonNullMembers)) {
          // a single non-null union member
          // this is how Avro represents "nullable" types; as a union of the NULL type with another
          // since ORC already supports nullability of all types, just use the child type directly
          return getOrcSchema(nonNullMembers.get(0));
        } else {
          // not a nullable union type; represent as an actual ORC union of them
          final TypeDescription union = TypeDescription.createUnion();
          for (final Schema childSchema : nonNullMembers) {
            union.addUnionChild(getOrcSchema(childSchema));
          }
          return union;
        }
      case STRING:
        return TypeDescription.createString();
      case FLOAT:
        return TypeDescription.createFloat();
      case DOUBLE:
        return TypeDescription.createDouble();
      case BOOLEAN:
        return TypeDescription.createBoolean();
      case ENUM:
        // represent as String for now
        return TypeDescription.createString();
      default:
        throw new IllegalStateException(String.format("Unrecognized Avro type: %s", type.getName()));
    }
  }

  /**
   * Get the {@link TypeDescription} for a binary schema type.
   *
   * This is based on logic from org.apache.hadoop.hive.serde2.avro.SchemaToTypeInfo#generateTypeInfo.
   *
   * @return If the logical type is decimal then return a decimal TypeDescription, otherwise return a binary
   * TypeDescription.
   *
   */
  private static TypeDescription getTypeDescriptionForBinarySchema(Schema avroSchema) {
    if ("decimal".equalsIgnoreCase(avroSchema.getProp("logicalType"))) {
      int scale = AvroSchemaUtils.getValueAsInteger(avroSchema, "scale");
      int precision = AvroSchemaUtils.getValueAsInteger(avroSchema, "precision");

      return TypeDescription.createDecimal().withScale(scale).withPrecision(precision);
    }

    return TypeDescription.createBinary();
  }

  /**
   * A helper method to check if the union is a nullable union. This check is to distinguish the case between a nullable and
   * a non-nullable union, each with a single member. In the former case, we want to "flatten" to the member type, while
   * in the case of the latter (i.e. non-nullable type), we want to preserve the union type.
   * @param unionSchema
   * @param nonNullMembers
   * @return true if the unionSchema is a nullable, false otherwise.
   */
  private static boolean isNullableUnion(Schema unionSchema, List<Schema> nonNullMembers) {
    return unionSchema.getTypes().size() == 2 && nonNullMembers.size() == 1;
  }

  /**
   * In Avro, a union defined with null in the first and only one type after is considered as a nullable
   * field instead of the real union type.
   *
   * For this type of schema, get all member types to help examine the real type of it.
   */
  public static List<Schema> getNonNullMembersOfUnion(Schema unionSchema) {
    return unionSchema.getTypes().stream().filter(schema -> !Schema.Type.NULL.equals(schema.getType()))
        .collect(Collectors.toList());
  }

  /**
   * Examine the Avro {@link Schema} object and get rid of "null" type in the beginning, which essentially indicates
   * the type is nullable. The elimination of null type from union member list is important to keep consistent with
   * {@link TypeDescription} object in terms of index of union member.
   */
  public static Schema sanitizeNullableSchema(Schema avroSchema) {
    if (avroSchema.getType() != Schema.Type.UNION) {
      return avroSchema;
    }

    // Processing union schema.
    List<Schema> members = getNonNullMembersOfUnion(avroSchema);
    if (isNullableUnion(avroSchema, members)) {
      return members.get(0);
    } else {
      // Reconstruct Avro Schema by eliminating null.
      return Schema.createUnion(members);
    }
  }
}
