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
package gobblin.recordaccess;

import java.util.Iterator;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import com.google.common.base.Optional;
import com.google.common.base.Splitter;

import gobblin.util.AvroUtils;


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
  public String getAsString(String fieldName) {
    Object val = getAsObject(fieldName);
    if (val == null) {
      return null;
    } else if (val instanceof Utf8) {
      return val.toString();
    } else {
      return (String)val;
    }
  }

  @Override
  public Integer getAsInt(String fieldName) {
    return (Integer)getAsObject(fieldName);
  }

  @Override
  public Long getAsLong(String fieldName) {
    Object val = getAsObject(fieldName);
    if (val instanceof Integer) {
      return ((Integer) val).longValue();
    } else {
      return (Long)val;
    }
  }

  private Object getAsObject(String fieldName) {
    Optional<Object> obj = AvroUtils.getFieldValue(record, fieldName);
    return obj.isPresent() ? obj.get() : null;
  }

  @Override
  public void set(String fieldName, String value) {
    set(fieldName, (Object)value);
  }

  @Override
  public void set(String fieldName, Integer value) {
    set(fieldName, (Object)value);
  }

  @Override
  public void set(String fieldName, Long value) {
    set(fieldName, (Object)value);
  }

  @Override
  public void setToNull(String fieldName) {
    set(fieldName, (Object)null);
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

      while (levels.hasNext()) {
        toInsert = (GenericRecord)toInsert.get(subField);
        if (toInsert == null) {
          throw new FieldDoesNotExistException("Field " + fieldName + " not found when trying to set " + fieldName);
        }
        subField = levels.next();
      }

      toInsert.put(subField, value);
    } catch (AvroRuntimeException e) {
      throw new FieldDoesNotExistException("Field not found setting name " + fieldName, e);
    }
  }
}
