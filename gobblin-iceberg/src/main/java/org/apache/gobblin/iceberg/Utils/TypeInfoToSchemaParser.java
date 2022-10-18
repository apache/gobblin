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

package org.apache.gobblin.iceberg.Utils;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.codehaus.jackson.node.JsonNodeFactory;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;



public final class TypeInfoToSchemaParser {
  private static final String DECIMAL_TYPE_NAME = "decimal";
  private static final String CHAR_TYPE_NAME = "char";
  private static final String VARCHAR_TYPE_NAME = "varchar";
  private static final String DATE_TYPE_NAME = "date";
  private static final String TIMESTAMP_TYPE_NAME = "timestamp-millis";
  private static final String AVRO_PROP_LOGICAL_TYPE = "logicalType";
  private static final String AVRO_PROP_PRECISION = "precision";
  private static final String AVRO_PROP_SCALE = "scale";
  private static final String AVRO_PROP_MAX_LENGTH = "maxLength";
  private static final String AVRO_STRING_TYPE_NAME = "string";
  private static final String AVRO_INT_TYPE_NAME = "int";
  private static final String AVRO_LONG_TYPE_NAME = "long";
  private static final String AVRO_SHORT_TYPE_NAME = "short";
  private static final String AVRO_BYTE_TYPE_NAME = "byte";
  private int _recordCounter = 0;
  private final String _namespace;
  private final boolean _mkFieldsOptional;
  private final Map<String, String> _downToUpCaseMap;

  public TypeInfoToSchemaParser(String namespace, boolean mkFieldsOptional, Map<String, String> downToUpCaseMap) {
    this._namespace = namespace;
    this._mkFieldsOptional = mkFieldsOptional;
    this._downToUpCaseMap = downToUpCaseMap;
  }

  public Schema parseSchemaFromFieldsTypeInfo(String recordNamespace, String recordName, List<String> fieldNames,
      List<TypeInfo> fieldTypeInfos) {
    List<Field> fields = new ArrayList();

    for (int i = 0; i < fieldNames.size(); ++i) {
      TypeInfo fieldTypeInfo = (TypeInfo) fieldTypeInfos.get(i);
      String fieldName = (String) fieldNames.get(i);
      fieldName = removePrefix(fieldName);
      fieldName = (String) this._downToUpCaseMap.getOrDefault(fieldName, fieldName);
      Schema schema = this.parseSchemaFromTypeInfo(fieldTypeInfo, recordNamespace + "." + recordName.toLowerCase(),
          StringUtils.capitalize(fieldName));
      Field f = AvroCompatibilityHelper.createSchemaField(fieldName, schema, null, null);
      fields.add(f);
    }

    Schema recordSchema = Schema.createRecord(recordName, (String) null, this._namespace + recordNamespace, false);
    recordSchema.setFields(fields);
    return recordSchema;
  }

  Schema parseSchemaFromTypeInfo(TypeInfo typeInfo, String recordNamespace, String recordName) {
    Category c = typeInfo.getCategory();
    Schema schema;
    switch (c) {
      case STRUCT:
        schema = this.parseSchemaFromStruct((StructTypeInfo) typeInfo, recordNamespace, recordName);
        break;
      case LIST:
        schema = this.parseSchemaFromList((ListTypeInfo) typeInfo, recordNamespace, recordName);
        break;
      case MAP:
        schema = this.parseSchemaFromMap((MapTypeInfo) typeInfo, recordNamespace, recordName);
        break;
      case PRIMITIVE:
        schema = this.parseSchemaFromPrimitive((PrimitiveTypeInfo) typeInfo);
        break;
      case UNION:
        schema = this.parseSchemaFromUnion((UnionTypeInfo) typeInfo, recordNamespace, recordName);
        break;
      default:
        throw new UnsupportedOperationException("Conversion from " + c + " not supported");
    }

    return this._mkFieldsOptional ? wrapInNullableUnion(schema) : schema;
  }

  private Schema parseSchemaFromUnion(UnionTypeInfo typeInfo, String recordNamespace, String recordName) {
    List<TypeInfo> typeInfos = typeInfo.getAllUnionObjectTypeInfos();
    List<Schema> schemas = new ArrayList();

    Schema candidate;
    for (Iterator var6 = typeInfos.iterator(); var6.hasNext();
        schemas.add(isNullableType(candidate) ? getOtherTypeFromNullableType(candidate) : candidate)) {
      TypeInfo ti = (TypeInfo) var6.next();
      if (ti instanceof StructTypeInfo) {
        StructTypeInfo sti = (StructTypeInfo) ti;
        String newRecordName = recordName + this._recordCounter;
        ++this._recordCounter;
        candidate = this.parseSchemaFromStruct(sti, recordNamespace, newRecordName);
      } else {
        candidate = this.parseSchemaFromTypeInfo(ti, recordNamespace, recordName);
      }
    }

    return Schema.createUnion(schemas);
  }

  private Schema parseSchemaFromStruct(StructTypeInfo typeInfo, String recordNamespace, String recordName) {
    Schema recordSchema =
        this.parseSchemaFromFieldsTypeInfo(recordNamespace, recordName, typeInfo.getAllStructFieldNames(),
            typeInfo.getAllStructFieldTypeInfos());
    return recordSchema;
  }

  private Schema parseSchemaFromList(ListTypeInfo typeInfo, String recordNamespace, String recordName) {
    Schema listSchema = this.parseSchemaFromTypeInfo(typeInfo.getListElementTypeInfo(), recordNamespace, recordName);
    return Schema.createArray(listSchema);
  }

  private Schema parseSchemaFromMap(MapTypeInfo typeInfo, String recordNamespace, String recordName) {
    TypeInfo keyTypeInfo = typeInfo.getMapKeyTypeInfo();
    PrimitiveCategory pc = ((PrimitiveTypeInfo) keyTypeInfo).getPrimitiveCategory();
    if (pc != PrimitiveCategory.STRING) {
      throw new UnsupportedOperationException("Key of Map can only be a String");
    } else {
      TypeInfo valueTypeInfo = typeInfo.getMapValueTypeInfo();
      Schema valueSchema = this.parseSchemaFromTypeInfo(valueTypeInfo, recordNamespace, recordName);
      return Schema.createMap(valueSchema);
    }
  }

  private Schema parseSchemaFromPrimitive(PrimitiveTypeInfo primitiveTypeInfo) {
    Schema schema;
    switch (primitiveTypeInfo.getPrimitiveCategory()) {
      case LONG:
        schema = Schema.create(Type.LONG);
        break;
      case DATE:
        schema = Schema.create(Type.INT);
        schema.addProp("logicalType", "date");
        break;
      case TIMESTAMP:
        schema = Schema.create(Type.LONG);
        schema.addProp("logicalType", "timestamp-millis");
        break;
      case BINARY:
        schema = Schema.create(Type.BYTES);
        break;
      case BOOLEAN:
        schema = Schema.create(Type.BOOLEAN);
        break;
      case DOUBLE:
        schema = Schema.create(Type.DOUBLE);
        break;
      case DECIMAL:
        DecimalTypeInfo dti = (DecimalTypeInfo) primitiveTypeInfo;
        JsonNodeFactory factory = JsonNodeFactory.instance;
        schema = Schema.create(Type.BYTES);
        schema.addProp("logicalType", "decimal");
        schema.addProp("precision", factory.numberNode(dti.getPrecision()));
        schema.addProp("scale", factory.numberNode(dti.getScale()));
        break;
      case FLOAT:
        schema = Schema.create(Type.FLOAT);
        break;
      case BYTE:
        schema = Schema.create(Type.INT);
        schema.addProp("logicalType", "byte");
        break;
      case SHORT:
        schema = Schema.create(Type.INT);
        schema.addProp("logicalType", "short");
        break;
      case INT:
        schema = Schema.create(Type.INT);
        break;
      case CHAR:
      case STRING:
      case VARCHAR:
        schema = Schema.create(Type.STRING);
        break;
      case VOID:
        schema = Schema.create(Type.NULL);
        break;
      default:
        throw new UnsupportedOperationException(primitiveTypeInfo + " is not supported.");
    }

    return schema;
  }

  @SuppressWarnings("checkstyle:FallThrough")
  private static Schema wrapInNullableUnion(Schema schema) {
    Schema wrappedSchema = schema;
    switch (schema.getType()) {
      case NULL:
        break;
      case UNION:
        List<Schema> unionSchemas = Lists.newArrayList(new Schema[]{Schema.create(Type.NULL)});
        unionSchemas.addAll(schema.getTypes());
        wrappedSchema = Schema.createUnion(unionSchemas);
        break;
      default:
        wrappedSchema = Schema.createUnion(Arrays.asList(Schema.create(Type.NULL), schema));
    }

    return wrappedSchema;
  }

  private static String removePrefix(String name) {
    int idx = name.lastIndexOf(46);
    return idx > 0 ? name.substring(idx + 1) : name;
  }

  private static boolean isNullableType(Schema schema) {
    if (!schema.getType().equals(Type.UNION)) {
      return false;
    } else {
      List<Schema> itemSchemas = schema.getTypes();
      if (itemSchemas.size() < 2) {
        return false;
      } else {
        Iterator var2 = itemSchemas.iterator();

        Schema itemSchema;
        do {
          if (!var2.hasNext()) {
            return false;
          }

          itemSchema = (Schema) var2.next();
        } while (!Type.NULL.equals(itemSchema.getType()));

        return true;
      }
    }
  }

  private static Schema getOtherTypeFromNullableType(Schema unionSchema) {
    List<Schema> types = unionSchema.getTypes();
    if (types.size() == 2) {
      if (((Schema) types.get(0)).getType() == Type.NULL) {
        return (Schema) types.get(1);
      } else {
        return ((Schema) types.get(1)).getType() == Type.NULL ? (Schema) types.get(0) : unionSchema;
      }
    } else {
      List<Schema> itemSchemas = new ArrayList();
      Iterator var3 = types.iterator();

      while (var3.hasNext()) {
        Schema itemSchema = (Schema) var3.next();
        if (!Type.NULL.equals(itemSchema.getType())) {
          itemSchemas.add(itemSchema);
        }
      }

      if (itemSchemas.size() > 1) {
        return Schema.createUnion(itemSchemas);
      } else {
        return (Schema) itemSchemas.get(0);
      }
    }
  }
}
