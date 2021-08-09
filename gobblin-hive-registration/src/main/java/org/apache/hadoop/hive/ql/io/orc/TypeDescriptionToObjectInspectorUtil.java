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

package org.apache.hadoop.hive.ql.io.orc;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.orc.OrcProto;
import org.apache.orc.OrcUtils;
import org.apache.orc.TypeDescription;


public class TypeDescriptionToObjectInspectorUtil {
  public static ObjectInspector getObjectInspector(TypeDescription orcSchema) {
    return createObjectInspector(0, OrcUtils.getOrcTypes(orcSchema));
  }

  static class Field implements StructField {
    private final String name;
    private final ObjectInspector inspector;
    private final int offset;

    Field(String name, ObjectInspector inspector, int offset) {
      this.name = name;
      this.inspector = inspector;
      this.offset = offset;
    }

    @Override
    public String getFieldName() {
      return name;
    }

    @Override
    public ObjectInspector getFieldObjectInspector() {
      return inspector;
    }

    @Override
    public int getFieldID() {
      return offset;
    }

    @Override
    public String getFieldComment() {
      return null;
    }
  }

  static class OrcStructInspector extends SettableStructObjectInspector {
    private List<StructField> fields;

    OrcStructInspector(int columnId, List<OrcProto.Type> types) {
      OrcProto.Type type = types.get(columnId);
      int fieldCount = type.getSubtypesCount();
      fields = new ArrayList<>(fieldCount);
      for(int i=0; i < fieldCount; ++i) {
        int fieldType = type.getSubtypes(i);
        fields.add(new Field(type.getFieldNames(i),
            createObjectInspector(fieldType, types), i));
      }
    }

    @Override
    public List<StructField> getAllStructFieldRefs() {
      return fields;
    }

    @Override
    public StructField getStructFieldRef(String s) {
      for(StructField field: fields) {
        if (field.getFieldName().equalsIgnoreCase(s)) {
          return field;
        }
      }
      return null;
    }

    @Override
    public Object getStructFieldData(Object object, StructField field) {
      if (object == null) {
        return null;
      }
      int offset = ((Field) field).offset;
      OrcStruct struct = (OrcStruct) object;
      if (offset >= struct.getNumFields()) {
        return null;
      }

      return struct.getFieldValue(offset);
    }

    @Override
    public List<Object> getStructFieldsDataAsList(Object object) {
      if (object == null) {
        return null;
      }
      OrcStruct struct = (OrcStruct) object;
      List<Object> result = new ArrayList<Object>(struct.getNumFields());
      for (int i=0; i<struct.getNumFields(); i++) {
        result.add(struct.getFieldValue(i));
      }
      return result;
    }

    @Override
    public String getTypeName() {
      StringBuilder buffer = new StringBuilder();
      buffer.append("struct<");
      for(int i=0; i < fields.size(); ++i) {
        StructField field = fields.get(i);
        if (i != 0) {
          buffer.append(",");
        }
        buffer.append(field.getFieldName());
        buffer.append(":");
        buffer.append(field.getFieldObjectInspector().getTypeName());
      }
      buffer.append(">");
      return buffer.toString();
    }

    @Override
    public Category getCategory() {
      return Category.STRUCT;
    }

    @Override
    public Object create() {
      return new OrcStruct(0);
    }

    @Override
    public Object setStructFieldData(Object struct, StructField field,
        Object fieldValue) {
      OrcStruct orcStruct = (OrcStruct) struct;
      int offset = ((Field) field).offset;
      // if the offset is bigger than our current number of fields, grow it
      if (orcStruct.getNumFields() <= offset) {
        orcStruct.setNumFields(offset+1);
      }
      orcStruct.setFieldValue(offset, fieldValue);
      return struct;
    }

  }

  static class OrcMapObjectInspector
      implements MapObjectInspector, SettableMapObjectInspector {
    private ObjectInspector key;
    private ObjectInspector value;

    OrcMapObjectInspector(int columnId, List<OrcProto.Type> types) {
      OrcProto.Type type = types.get(columnId);
      key = createObjectInspector(type.getSubtypes(0), types);
      value = createObjectInspector(type.getSubtypes(1), types);
    }

    @Override
    public ObjectInspector getMapKeyObjectInspector() {
      return key;
    }

    @Override
    public ObjectInspector getMapValueObjectInspector() {
      return value;
    }

    @Override
    public Object getMapValueElement(Object map, Object key) {
      return ((map == null || key == null)? null : ((Map) map).get(key));
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<Object, Object> getMap(Object map) {
      if (map == null) {
        return null;
      }
      return (Map) map;
    }

    @Override
    public int getMapSize(Object map) {
      if (map == null) {
        return -1;
      }
      return ((Map) map).size();
    }

    @Override
    public String getTypeName() {
      return "map<" + key.getTypeName() + "," + value.getTypeName() + ">";
    }

    @Override
    public Category getCategory() {
      return Category.MAP;
    }

    @Override
    public Object create() {
      return new LinkedHashMap<>();
    }

    @Override
    public Object put(Object map, Object key, Object value) {
      ((Map) map).put(key, value);
      return map;
    }

    @Override
    public Object remove(Object map, Object key) {
      ((Map) map).remove(key);
      return map;
    }

    @Override
    public Object clear(Object map) {
      ((Map) map).clear();
      return map;
    }

  }

  static class OrcUnionObjectInspector implements UnionObjectInspector {
    private List<ObjectInspector> children;

    protected OrcUnionObjectInspector() {
      super();
    }
    OrcUnionObjectInspector(int columnId,
        List<OrcProto.Type> types) {
      OrcProto.Type type = types.get(columnId);
      children = new ArrayList<ObjectInspector>(type.getSubtypesCount());
      for(int i=0; i < type.getSubtypesCount(); ++i) {
        children.add(createObjectInspector(type.getSubtypes(i),
            types));
      }
    }

    @Override
    public List<ObjectInspector> getObjectInspectors() {
      return children;
    }

    @Override
    public byte getTag(Object obj) {
      return ((OrcUnion) obj).getTag();
    }

    @Override
    public Object getField(Object obj) {
      return ((OrcUnion) obj).getObject();
    }

    @Override
    public String getTypeName() {
      StringBuilder builder = new StringBuilder("uniontype<");
      boolean first = true;
      for(ObjectInspector child: children) {
        if (first) {
          first = false;
        } else {
          builder.append(",");
        }
        builder.append(child.getTypeName());
      }
      builder.append(">");
      return builder.toString();
    }

    @Override
    public Category getCategory() {
      return Category.UNION;
    }

  }

  static class OrcListObjectInspector
      implements ListObjectInspector, SettableListObjectInspector {
    private ObjectInspector child;


    OrcListObjectInspector(int columnId, List<OrcProto.Type> types) {
      OrcProto.Type type = types.get(columnId);
      child = createObjectInspector(type.getSubtypes(0), types);
    }

    @Override
    public ObjectInspector getListElementObjectInspector() {
      return child;
    }

    @Override
    public Object getListElement(Object list, int i) {
      if (list == null || i < 0 || i >= getListLength(list)) {
        return null;
      }
      return ((List) list).get(i);
    }

    @Override
    public int getListLength(Object list) {
      if (list == null) {
        return -1;
      }
      return ((List) list).size();
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<?> getList(Object list) {
      if (list == null) {
        return null;
      }
      return (List) list;
    }

    @Override
    public String getTypeName() {
      return "array<" + child.getTypeName() + ">";
    }

    @Override
    public Category getCategory() {
      return Category.LIST;
    }

    @Override
    public Object create(int size) {
      ArrayList<Object> result = new ArrayList<Object>(size);
      for(int i = 0; i < size; ++i) {
        result.add(null);
      }
      return result;
    }

    @Override
    public Object set(Object list, int index, Object element) {
      List l = (List) list;
      for(int i=l.size(); i < index+1; ++i) {
        l.add(null);
      }
      l.set(index, element);
      return list;
    }

    @Override
    public Object resize(Object list, int newSize) {
      ((ArrayList) list).ensureCapacity(newSize);
      return list;
    }

  }


  static ObjectInspector createObjectInspector(int columnId,
      List<OrcProto.Type> types){
    OrcProto.Type type = types.get(columnId);
    switch (type.getKind()) {
      case FLOAT:
        return PrimitiveObjectInspectorFactory.writableFloatObjectInspector;
      case DOUBLE:
        return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
      case BOOLEAN:
        return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
      case BYTE:
        return PrimitiveObjectInspectorFactory.writableByteObjectInspector;
      case SHORT:
        return PrimitiveObjectInspectorFactory.writableShortObjectInspector;
      case INT:
        return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
      case LONG:
        return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
      case BINARY:
        return PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
      case STRING:
        return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
      case CHAR:
        if (!type.hasMaximumLength()) {
          throw new UnsupportedOperationException(
              "Illegal use of char type without length in ORC type definition.");
        }
        return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
            TypeInfoFactory.getCharTypeInfo(type.getMaximumLength()));
      case VARCHAR:
        if (!type.hasMaximumLength()) {
          throw new UnsupportedOperationException(
              "Illegal use of varchar type without length in ORC type definition.");
        }
        return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
            TypeInfoFactory.getVarcharTypeInfo(type.getMaximumLength()));
      case TIMESTAMP:
        return PrimitiveObjectInspectorFactory.writableTimestampObjectInspector;
      case DATE:
        return PrimitiveObjectInspectorFactory.writableDateObjectInspector;
      case DECIMAL:
        int precision = type.hasPrecision() ? type.getPrecision() : HiveDecimal.SYSTEM_DEFAULT_PRECISION;
        int scale =  type.hasScale()? type.getScale() : HiveDecimal.SYSTEM_DEFAULT_SCALE;
        return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
            TypeInfoFactory.getDecimalTypeInfo(precision, scale));
      case STRUCT:
        return new OrcStructInspector(columnId, types);
      case UNION:
        return new OrcUnionObjectInspector(columnId, types);
      case MAP:
        return new OrcMapObjectInspector(columnId, types);
      case LIST:
        return new OrcListObjectInspector(columnId, types);
      default:
        throw new UnsupportedOperationException("Unknown type " +
            type.getKind());
    }
  }
}
