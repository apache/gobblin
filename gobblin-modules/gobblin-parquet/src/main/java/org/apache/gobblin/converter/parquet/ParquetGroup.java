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
package org.apache.gobblin.converter.parquet;

import java.util.ArrayList;
import java.util.List;

import parquet.example.data.Group;
import parquet.example.data.simple.BinaryValue;
import parquet.example.data.simple.BooleanValue;
import parquet.example.data.simple.DoubleValue;
import parquet.example.data.simple.FloatValue;
import parquet.example.data.simple.Int96Value;
import parquet.example.data.simple.IntegerValue;
import parquet.example.data.simple.LongValue;
import parquet.example.data.simple.NanoTime;
import parquet.example.data.simple.Primitive;
import parquet.io.api.Binary;
import parquet.io.api.RecordConsumer;
import parquet.schema.GroupType;
import parquet.schema.PrimitiveType;
import parquet.schema.Type;

import static parquet.schema.Type.Repetition.REPEATED;


/**
 * Custom Implementation of {@link Group} to support adding {@link Object} of type {@link Primitive} or {@link Group}.
 * Also provides methods to add {@link Primitive} and {@link Group} with {@link String} key if index is not known.
 * @author tilakpatidar
 */
public class ParquetGroup extends Group {

  private final GroupType schema;
  //each item represents data of a field, which is indexed by the fieldIndex of the schema
  private final List<Object>[] data;

  public ParquetGroup(GroupType schema) {
    this.schema = schema;
    this.data = new List[schema.getFields().size()];

    for (int i = 0; i < schema.getFieldCount(); ++i) {
      this.data[i] = new ArrayList();
    }
  }

  public String toString() {
    return this.toString("");
  }

  public String toString(String indent) {
    StringBuilder result = new StringBuilder();
    int i = 0;
    for (Type field : this.schema.getFields()) {
      String name = field.getName();
      List<Object> values = this.data[i];
      for (Object value : values) {
        result.append(indent).append(name);
        if (value == null) {
          result.append(": NULL\n");
        } else if (value instanceof Group) {
          result.append("\n").append(((ParquetGroup) value).toString(indent + "  "));
        } else {
          result.append(": ").append(value.toString()).append("\n");
        }
      }
      i++;
    }
    return result.toString();
  }

  public Group addGroup(int fieldIndex) {
    ParquetGroup g = new ParquetGroup(this.schema.getType(fieldIndex).asGroupType());
    this.data[fieldIndex].add(g);
    return g;
  }

  public Group getGroup(int fieldIndex, int index) {
    return (Group) this.getValue(fieldIndex, index);
  }

  private Object getValue(int fieldIndex, int index) {
    List<Object> list;
    try {
      list = this.data[fieldIndex];
    } catch (IndexOutOfBoundsException var6) {
      throw new RuntimeException(
          "not found " + fieldIndex + "(" + this.schema.getFieldName(fieldIndex) + ") in group:\n" + this);
    }

    try {
      return list.get(index);
    } catch (IndexOutOfBoundsException var5) {
      throw new RuntimeException(
          "not found " + fieldIndex + "(" + this.schema.getFieldName(fieldIndex) + ") element number " + index
              + " in group:\n" + this);
    }
  }

  public void add(int fieldIndex, Primitive value) {
    Type type = this.schema.getType(fieldIndex);
    List<Object> list = this.data[fieldIndex];
    if (!type.isRepetition(REPEATED) && !list.isEmpty()) {
      throw new IllegalStateException(
          "field " + fieldIndex + " (" + type.getName() + ") can not have more than one value: " + list);
    } else {
      list.add(value);
    }
  }

  public int getFieldRepetitionCount(int fieldIndex) {
    List<Object> list = this.data[fieldIndex];
    return list == null ? 0 : list.size();
  }

  public String getValueToString(int fieldIndex, int index) {
    return String.valueOf(this.getValue(fieldIndex, index));
  }

  public String getString(int fieldIndex, int index) {
    return ((BinaryValue) this.getValue(fieldIndex, index)).getString();
  }

  public int getInteger(int fieldIndex, int index) {
    return ((IntegerValue) this.getValue(fieldIndex, index)).getInteger();
  }

  public boolean getBoolean(int fieldIndex, int index) {
    return ((BooleanValue) this.getValue(fieldIndex, index)).getBoolean();
  }

  public Binary getBinary(int fieldIndex, int index) {
    return ((BinaryValue) this.getValue(fieldIndex, index)).getBinary();
  }

  public Binary getInt96(int fieldIndex, int index) {
    return ((Int96Value) this.getValue(fieldIndex, index)).getInt96();
  }

  public void add(int fieldIndex, int value) {
    this.add(fieldIndex, new IntegerValue(value));
  }

  public void add(int fieldIndex, long value) {
    this.add(fieldIndex, new LongValue(value));
  }

  public void add(int fieldIndex, String value) {
    this.add(fieldIndex, new BinaryValue(Binary.fromString(value)));
  }

  public void add(int fieldIndex, NanoTime value) {
    this.add(fieldIndex, value.toInt96());
  }

  public void add(int fieldIndex, boolean value) {
    this.add(fieldIndex, new BooleanValue(value));
  }

  public void add(int fieldIndex, Binary value) {
    switch (this.getType().getType(fieldIndex).asPrimitiveType().getPrimitiveTypeName()) {
      case BINARY:
        this.add(fieldIndex, new BinaryValue(value));
        break;
      case INT96:
        this.add(fieldIndex, new Int96Value(value));
        break;
      default:
        throw new UnsupportedOperationException(
            this.getType().asPrimitiveType().getName() + " not supported for Binary");
    }
  }

  public void add(int fieldIndex, float value) {
    this.add(fieldIndex, new FloatValue(value));
  }

  public void add(int fieldIndex, double value) {
    this.add(fieldIndex, new DoubleValue(value));
  }

  public GroupType getType() {
    return this.schema;
  }

  public void writeValue(int field, int index, RecordConsumer recordConsumer) {
    ((Primitive) this.getValue(field, index)).writeValue(recordConsumer);
  }

  /**
   * Add any object of {@link PrimitiveType} or {@link Group} type with a String key.
   * @param key
   * @param object
   */
  public void add(String key, Object object) {
    int fieldIndex = getIndex(key);
    if (object.getClass() == ParquetGroup.class) {
      this.addGroup(key, (Group) object);
    } else {
      this.add(fieldIndex, (Primitive) object);
    }
  }

  private int getIndex(String key) {
    return getType().getFieldIndex(key);
  }

  /**
   * Add a {@link Group} given a String key.
   * @param key
   * @param object
   */
  private void addGroup(String key, Group object) {
    int fieldIndex = getIndex(key);
    this.schema.getType(fieldIndex).asGroupType();
    this.data[fieldIndex].add(object);
  }
}

