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
package org.apache.gobblin.writer;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.gobblin.util.orc.AvroOrcSchemaConverter;
import org.apache.orc.TypeDescription;
import org.apache.orc.storage.common.type.HiveDecimal;
import org.apache.orc.storage.ql.exec.vector.BytesColumnVector;
import org.apache.orc.storage.ql.exec.vector.ColumnVector;
import org.apache.orc.storage.ql.exec.vector.DecimalColumnVector;
import org.apache.orc.storage.ql.exec.vector.DoubleColumnVector;
import org.apache.orc.storage.ql.exec.vector.ListColumnVector;
import org.apache.orc.storage.ql.exec.vector.LongColumnVector;
import org.apache.orc.storage.ql.exec.vector.MapColumnVector;
import org.apache.orc.storage.ql.exec.vector.StructColumnVector;
import org.apache.orc.storage.ql.exec.vector.UnionColumnVector;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;

import com.google.common.annotations.VisibleForTesting;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.State;


/**
 * The converter for buffering rows and forming columnar batch.
 */
@Slf4j
public class GenericRecordToOrcValueWriter implements OrcValueWriter<GenericRecord> {
  private static final String ENABLE_SMART_ARRAY_ENLARGE = GobblinOrcWriter.ORC_WRITER_PREFIX + "enabledMulValueColumnVectorSmartSizing";
  private static final boolean DEFAULT_ENABLE_SMART_ARRAY_ENLARGE = false;
  private static final String ENLARGE_FACTOR_KEY = GobblinOrcWriter.ORC_WRITER_PREFIX + "enlargeFactor";
  private static final int DEFAULT_ENLARGE_FACTOR = 3;

  private boolean enabledSmartSizing;
  private int enlargeFactor;

  // A rough measure of how many times resize is triggered, helping on debugging and testing.
  @VisibleForTesting
  public int resizeCount = 0;

  /**
   * The interface for the conversion from GenericRecord to ORC's ColumnVectors.
   */
  interface Converter {
    /**
     * Take a value from the Generic record data value and add it to the ORC output.
     * @param rowId the row in the ColumnVector
     * @param column either the column number or element number
     * @param data Object which contains the data
     * @param output the ColumnVector to put the value into
     */
    void addValue(int rowId, int column, Object data, ColumnVector output);
  }

  private final Converter[] converters;

  public GenericRecordToOrcValueWriter(TypeDescription typeDescription, Schema avroSchema) {
    converters = buildConverters(typeDescription, avroSchema);
    this.enabledSmartSizing = DEFAULT_ENABLE_SMART_ARRAY_ENLARGE;
    this.enlargeFactor = DEFAULT_ENLARGE_FACTOR;
  }

  public GenericRecordToOrcValueWriter(TypeDescription typeDescription, Schema avroSchema, State state) {
    this(typeDescription, avroSchema);
    this.enabledSmartSizing = state.getPropAsBoolean(ENABLE_SMART_ARRAY_ENLARGE, DEFAULT_ENABLE_SMART_ARRAY_ENLARGE);
    this.enlargeFactor = state.getPropAsInt(ENLARGE_FACTOR_KEY, DEFAULT_ENLARGE_FACTOR);
    log.info("enabledSmartSizing: {}, enlargeFactor: {}", enabledSmartSizing, enlargeFactor);
  }

  @Override
  public void write(GenericRecord value, VectorizedRowBatch output)
      throws IOException {

    int row = output.size++;
    for (int c = 0; c < converters.length; ++c) {
      ColumnVector col = output.cols[c];
      if (value.get(c) == null) {
        col.noNulls = false;
        col.isNull[row] = true;
      } else {
        col.isNull[row] = false;
        converters[c].addValue(row, c, value.get(c), col);
      }
    }
  }

  static class BooleanConverter implements Converter {
    public void addValue(int rowId, int column, Object data, ColumnVector output) {
      ((LongColumnVector) output).vector[rowId] = (boolean) data ? 1 : 0;
    }
  }

  static class ByteConverter implements Converter {
    public void addValue(int rowId, int column, Object data, ColumnVector output) {
      ((LongColumnVector) output).vector[rowId] = (byte) data;
    }
  }

  static class ShortConverter implements Converter {
    public void addValue(int rowId, int column, Object data, ColumnVector output) {
      ((LongColumnVector) output).vector[rowId] = (short) data;
    }
  }

  static class IntConverter implements Converter {
    public void addValue(int rowId, int column, Object data, ColumnVector output) {
      ((LongColumnVector) output).vector[rowId] = (int) data;
    }
  }

  static class LongConverter implements Converter {
    public void addValue(int rowId, int column, Object data, ColumnVector output) {
      ((LongColumnVector) output).vector[rowId] = (long) data;
    }
  }

  static class FloatConverter implements Converter {
    public void addValue(int rowId, int column, Object data, ColumnVector output) {
      ((DoubleColumnVector) output).vector[rowId] = (float) data;
    }
  }

  static class DoubleConverter implements Converter {
    public void addValue(int rowId, int column, Object data, ColumnVector output) {
      ((DoubleColumnVector) output).vector[rowId] = (double) data;
    }
  }

  static class StringConverter implements Converter {
    public void addValue(int rowId, int column, Object data, ColumnVector output) {
      final byte[] value;
      if (data instanceof GenericEnumSymbol) {
        value = data.toString().getBytes(StandardCharsets.UTF_8);
      } else if (data instanceof Enum) {
        value = ((Enum) data).name().getBytes(StandardCharsets.UTF_8);
      } else if (data instanceof Utf8) {
        value = ((Utf8) data).getBytes();
      } else {
        value = ((String) data).getBytes(StandardCharsets.UTF_8);
      }
      ((BytesColumnVector) output).setRef(rowId, value, 0, value.length);
    }
  }

  static class BytesConverter implements Converter {
    public void addValue(int rowId, int column, Object data, ColumnVector output) {
      final byte[] value;
      if (data instanceof GenericFixed) {
        value = ((GenericFixed) data).bytes();
      } else if (data instanceof ByteBuffer) {
        value = ((ByteBuffer) data).array();
      } else {
        value = (byte[]) data;
      }
      ((BytesColumnVector) output).setRef(rowId, value, 0, value.length);
    }
  }

  static class DecimalConverter implements Converter {
    private final int scale;

    public DecimalConverter(int scale) {
      this.scale = scale;
    }

    public void addValue(int rowId, int column, Object data, ColumnVector output) {
      ((DecimalColumnVector) output).vector[rowId].set(getHiveDecimalFromByteBuffer((ByteBuffer) data));
    }

    /**
     * Based on logic from org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils
     */
    private byte[] getBytesFromByteBuffer(ByteBuffer byteBuffer) {
      byteBuffer.rewind();
      byte[] result = new byte[byteBuffer.limit()];
      byteBuffer.get(result);
      return result;
    }

    /**
     * Based on logic from org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils
     */
    private HiveDecimal getHiveDecimalFromByteBuffer(ByteBuffer byteBuffer) {
      byte[] result = getBytesFromByteBuffer(byteBuffer);

      return HiveDecimal.create(new BigInteger(result), this.scale);
    }
  }

  class StructConverter implements Converter {
    private final Converter[] children;

    StructConverter(TypeDescription schema, Schema avroSchema) {
      children = new Converter[schema.getChildren().size()];
      for (int c = 0; c < children.length; ++c) {
        children[c] = buildConverter(schema.getChildren().get(c), avroSchema.getFields().get(c).schema());
      }
    }

    public void addValue(int rowId, int column, Object data, ColumnVector output) {
      GenericRecord value = (GenericRecord) data;
      StructColumnVector cv = (StructColumnVector) output;
      for (int c = 0; c < children.length; ++c) {
        ColumnVector field = cv.fields[c];
        if (value.get(c) == null) {
          field.noNulls = false;
          field.isNull[rowId] = true;
        } else {
          field.isNull[rowId] = false;
          children[c].addValue(rowId, c, value.get(c), field);
        }
      }
    }
  }

  class UnionConverter implements Converter {
    private final Converter[] children;
    private final Schema unionSchema;

    UnionConverter(TypeDescription schema, Schema avroSchema) {
      children = new Converter[schema.getChildren().size()];
      for (int c = 0; c < children.length; ++c) {
        children[c] = buildConverter(schema.getChildren().get(c), avroSchema.getTypes().get(c));
      }
      this.unionSchema = avroSchema;
    }

    /**
     * @param data Object which contains the data, for Union, this data object is already the
     *             original data type without union wrapper.
     */
    @Override
    public void addValue(int rowId, int column, Object data, ColumnVector output) {
      UnionColumnVector cv = (UnionColumnVector) output;
      int tag = (data != null) ? GenericData.get().resolveUnion(unionSchema, data) : children.length;

      for (int c = 0; c < children.length; ++c) {
        ColumnVector field = cv.fields[c];
        // If c == tag that indicates data must not be null
        if (c == tag) {
          field.isNull[rowId] = false;
          cv.tags[rowId] = c;
          children[c].addValue(rowId, c, data, field);
        } else {
          field.noNulls = false;
          field.isNull[rowId] = true;
        }
      }
    }
  }

  class ListConverter implements Converter {
    private final Converter children;
    // Keep track of total number of rows being added to help calculate row's avg size.
    private int rowsAdded;

    ListConverter(TypeDescription schema, Schema avroSchema) {
      children = buildConverter(schema.getChildren().get(0), avroSchema.getElementType());
      rowsAdded = 0;
    }

    public void addValue(int rowId, int column, Object data, ColumnVector output) {
      rowsAdded += 1;
      List value = (List) data;
      ListColumnVector cv = (ListColumnVector) output;

      // record the length and start of the list elements
      cv.lengths[rowId] = value.size();
      cv.offsets[rowId] = cv.childCount;
      cv.childCount += cv.lengths[rowId];
      // make sure the child is big enough
      // If seeing child array being saturated, will need to expand with a reasonable amount.
      if (cv.childCount > cv.child.isNull.length) {
        int resizedLength = resize(rowsAdded, cv.isNull.length, cv.childCount);
        log.info("Column vector: {}, resizing to: {}, child count: {}", cv.child, resizedLength, cv.childCount);
        cv.child.ensureSize(resizedLength, true);
      }

      // Add each element
      for (int e = 0; e < cv.lengths[rowId]; ++e) {
        int offset = (int) (e + cv.offsets[rowId]);
        if (value.get(e) == null) {
          cv.child.noNulls = false;
          cv.child.isNull[offset] = true;
        } else {
          cv.child.isNull[offset] = false;
          children.addValue(offset, e, value.get(e), cv.child);
        }
      }
    }
  }

  class MapConverter implements Converter {
    private final Converter keyConverter;
    private final Converter valueConverter;
    // Keep track of total number of rows being added to help calculate row's avg size.
    private int rowsAdded;

    MapConverter(TypeDescription schema, Schema avroSchema) {
      keyConverter = buildConverter(schema.getChildren().get(0), SchemaBuilder.builder().stringType());
      valueConverter = buildConverter(schema.getChildren().get(1), avroSchema.getValueType());
      rowsAdded = 0;
    }

    public void addValue(int rowId, int column, Object data, ColumnVector output) {
      rowsAdded += 1;
      Map<Object, Object> map = (Map<Object, Object>) data;
      Set<Map.Entry<Object, Object>> entries = map.entrySet();
      MapColumnVector cv = (MapColumnVector) output;

      // record the length and start of the list elements
      cv.lengths[rowId] = entries.size();
      cv.offsets[rowId] = cv.childCount;
      cv.childCount += cv.lengths[rowId];
      // make sure the child is big enough
      if (cv.childCount > cv.keys.isNull.length) {
        int resizedLength = resize(rowsAdded, cv.isNull.length, cv.childCount);
        log.info("Column vector: {}, resizing to: {}, child count: {}", cv.keys, resizedLength, cv.childCount);
        cv.keys.ensureSize(resizedLength, true);
        log.info("Column vector: {}, resizing to: {}, child count: {}", cv.values, resizedLength, cv.childCount);
        cv.values.ensureSize(resizedLength, true);
      }
      // Add each element
      int e = 0;
      for (Map.Entry entry : entries) {
        int offset = (int) (e + cv.offsets[rowId]);
        if (entry.getKey() == null) {
          cv.keys.noNulls = false;
          cv.keys.isNull[offset] = true;
        } else {
          cv.keys.isNull[offset] = false;
          keyConverter.addValue(offset, e, entry.getKey(), cv.keys);
        }
        if (entry.getValue() == null) {
          cv.values.noNulls = false;
          cv.values.isNull[offset] = true;
        } else {
          cv.values.isNull[offset] = false;
          valueConverter.addValue(offset, e, entry.getValue(), cv.values);
        }
        e++;
      }
    }
  }

  /**
   * Resize the child-array size based on configuration.
   * If smart-sizing is enabled, it will using the avg size of container and expand the whole child array to
   * delta(avgSizeOfContainer * numberOfContainer(batchSize)) the first time this is called.
   * If there's further resize requested, it will add delta again to be conservative, but chances of adding delta
   * for multiple times should be low, unless the container size is fluctuating too much.
   */
  private int resize(int rowsAdded, int batchSize, int requestedSize) {
    resizeCount += 1;
    log.info(String.format("It has been resized %s times in current writer", resizeCount));
    return enabledSmartSizing ? requestedSize + (requestedSize / rowsAdded + 1) * batchSize : enlargeFactor * requestedSize;
  }

  private Converter buildConverter(TypeDescription schema, Schema avroSchema) {
    switch (schema.getCategory()) {
      case BOOLEAN:
        return new BooleanConverter();
      case BYTE:
        return new ByteConverter();
      case SHORT:
        return new ShortConverter();
      case INT:
        return new IntConverter();
      case LONG:
        return new LongConverter();
      case FLOAT:
        return new FloatConverter();
      case DOUBLE:
        return new DoubleConverter();
      case BINARY:
        return new BytesConverter();
      case STRING:
      case CHAR:
      case VARCHAR:
        return new StringConverter();
      case DECIMAL:
        return new DecimalConverter(schema.getScale());
      case STRUCT:
        return new StructConverter(schema, AvroOrcSchemaConverter.sanitizeNullableSchema(avroSchema));
      case LIST:
        return new ListConverter(schema, AvroOrcSchemaConverter.sanitizeNullableSchema(avroSchema));
      case MAP:
        return new MapConverter(schema, AvroOrcSchemaConverter.sanitizeNullableSchema(avroSchema));
      case UNION:
        return new UnionConverter(schema, AvroOrcSchemaConverter.sanitizeNullableSchema(avroSchema));
      default:
        throw new IllegalArgumentException("Unhandled type " + schema);
    }
  }

  private Converter[] buildConverters(TypeDescription schema, Schema avroSchema) {
    if (schema.getCategory() != TypeDescription.Category.STRUCT) {
      throw new IllegalArgumentException("Top level must be a struct " + schema);
    }
    List<TypeDescription> children = schema.getChildren();
    Converter[] result = new Converter[children.size()];
    for (int c = 0; c < children.size(); ++c) {
      result[c] = buildConverter(children.get(c), avroSchema.getFields().get(c).schema());
    }
    return result;
  }
}