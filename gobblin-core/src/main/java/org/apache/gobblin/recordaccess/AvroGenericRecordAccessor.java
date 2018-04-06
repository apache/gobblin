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
package org.apache.gobblin.recordaccess;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import com.google.common.base.Optional;
import com.google.common.base.Splitter;

import org.apache.gobblin.util.AvroUtils;


/**
 * Implementation of a RecordAccessor that can process Avro GenericRecords.
 *
 * NOTE: This class assumes field names never contain a '.'; it assumes they are always
 * nested.
 */
public class AvroGenericRecordAccessor implements RecordAccessor {
  private final GenericRecord record;

  public AvroGenericRecordAccessor(GenericRecord record) {
    this.record = record;
  }

  @Override
  public Map<String, String> getMultiAsString(String fieldName) {
    Map<String, Object> vals = getMultiGeneric(fieldName);
    Map<String, String> ret = new HashMap<>();

    for (Map.Entry<String, Object> entry : vals.entrySet()) {
      Object val = entry.getValue();
      String convertedVal = convertToString(entry.getKey(), val);

      if (convertedVal != null) {
        ret.put(entry.getKey(), convertedVal);
      }
    }

    return ret;
  }

  @Override
  public String getAsString(String fieldName) {
    Object obj = getAsObject(fieldName);
    return convertToString(fieldName, obj);
  }


  private String convertToString(String fieldName, Object obj) {
    if (obj == null) {
      return null;
    } else if (obj instanceof String) {
      return (String)obj;
    } else if (obj instanceof Utf8) {
      return obj.toString();
    } else {
      return castOrThrowTypeException(fieldName, obj, String.class);
    }
  }

  @Override
  public Map<String, Integer> getMultiAsInt(String fieldName) {
    Map<String, Object> vals = getMultiGeneric(fieldName);
    Map<String, Integer> ret = new HashMap<>();

    for (Map.Entry<String, Object> entry : vals.entrySet()) {
      Object val = entry.getValue();
      Integer convertedVal = convertToInt(entry.getKey(), val);

      if (convertedVal != null) {
        ret.put(entry.getKey(), convertedVal);
      }
    }

    return ret;
  }

  @Override
  public Integer getAsInt(String fieldName) {
    return convertToInt(fieldName, getAsObject(fieldName));
  }

  private Integer convertToInt(String fieldName, Object obj) {
    return castOrThrowTypeException(fieldName, obj, Integer.class);
  }

  @Override
  public Map<String, Long> getMultiAsLong(String fieldName) {
    Map<String, Object> vals = getMultiGeneric(fieldName);
    Map<String, Long> ret = new HashMap<>();

    for (Map.Entry<String, Object> entry : vals.entrySet()) {
      Object val = entry.getValue();
      Long convertedVal = convertToLong(entry.getKey(), val);

      if (convertedVal != null) {
        ret.put(entry.getKey(), convertedVal);
      }
    }

    return ret;
  }

  @Override
  public Long getAsLong(String fieldName) {
    return convertToLong(fieldName, getAsObject(fieldName));
  }

  private Long convertToLong(String fieldName, Object obj) {
    if (obj instanceof Integer) {
      return ((Integer) obj).longValue();
    } else {
      return castOrThrowTypeException(fieldName, obj, Long.class);
    }
  }

  private <T> T castOrThrowTypeException(String fieldName, Object o, Class<? extends T> clazz) {
    try {
      if (o == null) {
        return null;
      }
      return clazz.cast(o);
    } catch (ClassCastException e) {
      throw new IncorrectTypeException("Incorrect type for field " + fieldName, e);
    }
  }

  private Object getAsObject(String fieldName) {
    Optional<Object> obj = AvroUtils.getFieldValue(record, fieldName);
    return obj.isPresent() ? obj.get() : null;
  }

  @Override
  public Map<String, Object> getMultiGeneric(String fieldName) {
    Map<String, Object> vals = AvroUtils.getMultiFieldValue(record, fieldName);
    for (Map.Entry<String, Object> entry: vals.entrySet()) {
      vals.put(entry.getKey(), convertAvroToJava(entry.getKey(), entry.getValue()));
    }

    return vals;
  }

  @Override
  public Object getGeneric(String fieldName) {
    Object val = getAsObject(fieldName);
    return convertAvroToJava(fieldName, val);
  }

  private Object convertAvroToJava(String fieldName, Object val) {
    if (val == null || val instanceof String || val instanceof Long || val instanceof Integer) {
      return val;
    }

    if (val instanceof Utf8) {
      return convertToString(fieldName, val);
    }

    if (val instanceof GenericArray) {
      return convertToList(fieldName, (GenericArray) val);
    }

    throw new IllegalArgumentException("Don't know how to parse object of type " + val.getClass().getCanonicalName());
  }

  @Override
  public void set(String fieldName, String value) {
    set(fieldName, (Object) value);
  }

  @Override
  public void set(String fieldName, Integer value) {
    set(fieldName, (Object) value);
  }

  @Override
  public void set(String fieldName, Long value) {
    set(fieldName, (Object) value);
  }

  @Override
  public void setStringArray(String fieldName, List<String> value) {
    GenericData.Array<String> avroArray = new GenericData.Array<>(
        Schema.createArray(Schema.create(Schema.Type.STRING)), value);
    set(fieldName, avroArray);
  }

  @Override
  public void setToNull(String fieldName) {
    set(fieldName, (Object) null);
  }

  /*
   * Recurse down record types to set the right value
   */
  private void set(String fieldName, Object value) {
    try {
      String subField;
      Iterator<String> levels = Splitter.on(".").split(fieldName).iterator();
      GenericRecord toInsert = record;
      subField = levels.next();
      Object subRecord = toInsert;

      while (levels.hasNext()) {
        if (subRecord instanceof GenericRecord) {
          subRecord = ((GenericRecord)subRecord).get(subField);
        } else if (subRecord instanceof List) {
          subRecord = ((List)subRecord).get(Integer.parseInt(subField));
        } else if (subRecord instanceof Map) {
          subRecord = ((Map)subRecord).get(subField);
        }

        if (subRecord == null) {
          throw new FieldDoesNotExistException("Field " + subField + " not found when trying to set " + fieldName);
        }
        subField = levels.next();
      }

      if (!(subRecord instanceof GenericRecord)) {
        throw new IllegalArgumentException("Field " + fieldName + " does not refer to a record type.");
      }

      toInsert = (GenericRecord)subRecord;
      Object oldValue = toInsert.get(subField);

      toInsert.put(subField, value);

      Schema.Field changedField = toInsert.getSchema().getField(subField);
      GenericData genericData = GenericData.get();

      boolean valid = genericData
          .validate(changedField.schema(), genericData.getField(toInsert, changedField.name(), changedField.pos()));
      if (!valid) {
        toInsert.put(subField, oldValue);
        throw new IncorrectTypeException(
            "Incorrect type - can't insert a " + value.getClass().getCanonicalName() + " into an Avro record of type "
                + changedField.schema().getType().toString());
      }
    } catch (AvroRuntimeException e) {
      throw new FieldDoesNotExistException("Field not found setting name " + fieldName, e);
    }
  }

  @SuppressWarnings("unchecked")
  private List convertToList(String fieldName, GenericArray arr) {
    List ret = new ArrayList();
    for (int i = 0; i < arr.size(); i++) {
      ret.add(convertAvroToJava(fieldName + "." + String.valueOf(i), arr.get(i)));
    }

    return ret;
  }
}
