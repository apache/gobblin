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

import com.google.common.base.Preconditions;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.State;

/**
 * A helper class to calculate the size of array buffers in a {@link VectorizedRowBatch}.
 * This estimate is mainly based on the maximum size of each variable length column, which can be resized
 * Since the resizing algorithm for each column can balloon, this can affect likelihood of OOM
 */
@Slf4j
public class OrcConverterMemoryManager {

  private static final boolean DEFAULT_ENABLE_SMART_ARRAY_ENLARGE = false;
  private static final int DEFAULT_ENLARGE_FACTOR = 3;
  private static final double DEFAULT_SMART_ARRAY_ENLARGE_FACTOR_MAX = 5.0;
  // Needs to be greater than 1.0
  private static final double DEFAULT_SMART_ARRAY_ENLARGE_FACTOR_MIN = 1.2;
  // Given the defaults it will take around 500 records to reach the min enlarge factor - given that the default
  // batch size is 1000 this is a fairly conservative default
  private static final double DEFAULT_SMART_ARRAY_ENLARGE_DECAY_FACTOR = 0.003;

  private VectorizedRowBatch rowBatch;
  @Getter
  private int resizeCount = 0;
  private double smartArrayEnlargeFactorMax;
  private double smartArrayEnlargeFactorMin;
  private double smartArrayEnlargeDecayFactor;
  private boolean enabledSmartSizing = false;
  int enlargeFactor = 0;

  OrcConverterMemoryManager(VectorizedRowBatch rowBatch, State state) {
    this.rowBatch = rowBatch;
    this.enabledSmartSizing = state.getPropAsBoolean(GobblinOrcWriterConfigs.ENABLE_SMART_ARRAY_ENLARGE, DEFAULT_ENABLE_SMART_ARRAY_ENLARGE);
    this.enlargeFactor = state.getPropAsInt(GobblinOrcWriterConfigs.ENLARGE_FACTOR_KEY, DEFAULT_ENLARGE_FACTOR);
    this.smartArrayEnlargeFactorMax = state.getPropAsDouble(GobblinOrcWriterConfigs.SMART_ARRAY_ENLARGE_FACTOR_MAX, DEFAULT_SMART_ARRAY_ENLARGE_FACTOR_MAX);
    this.smartArrayEnlargeFactorMin = state.getPropAsDouble(GobblinOrcWriterConfigs.SMART_ARRAY_ENLARGE_FACTOR_MIN, DEFAULT_SMART_ARRAY_ENLARGE_FACTOR_MIN);
    this.smartArrayEnlargeDecayFactor = state.getPropAsDouble(GobblinOrcWriterConfigs.SMART_ARRAY_ENLARGE_DECAY_FACTOR, DEFAULT_SMART_ARRAY_ENLARGE_DECAY_FACTOR);
    if (enabledSmartSizing) {
      Preconditions.checkArgument(this.smartArrayEnlargeFactorMax >= 1,
          String.format("Smart array enlarge factor needs to be larger than 1.0, provided value %s", this.smartArrayEnlargeFactorMax));
      Preconditions.checkArgument(this.smartArrayEnlargeFactorMin >= 1,
          String.format("Smart array enlarge factor needs to be larger than 1.0, provided value %s", this.smartArrayEnlargeFactorMin));
      Preconditions.checkArgument(this.smartArrayEnlargeDecayFactor > 0 && this.smartArrayEnlargeDecayFactor < 1,
          String.format("Smart array enlarge decay factor needs to be between 0 and 1, provided value %s", this.smartArrayEnlargeDecayFactor));
      log.info("Enabled smart resizing for rowBatch - smartArrayEnlargeFactorMax: {}, smartArrayEnlargeFactorMin: {}, smartArrayEnlargeDecayFactor: {}",
          smartArrayEnlargeFactorMax, smartArrayEnlargeFactorMin, smartArrayEnlargeDecayFactor);
    }
    log.info("Static enlargeFactor for rowBatch: {}", enlargeFactor);
  }

  /**
   * Estimates the approximate size in bytes of elements in a column
   * This takes into account the default null values of different ORC ColumnVectors and approximates their sizes
   * Although its a rough calculation, it can accurately depict the weight of resizes in a column for very large arrays and maps
   * Which tend to dominate the size of the buffer overall
   * @param col
   * @return
   */
  public long calculateSizeOfColHelper(ColumnVector col) {
    long converterBufferColSize = 0;
    switch (col.type) {
      case LIST:
        ListColumnVector listColumnVector = (ListColumnVector) col;
        converterBufferColSize += calculateSizeOfColHelper(listColumnVector.child);
        break;
      case MAP:
        MapColumnVector mapColumnVector = (MapColumnVector) col;
        converterBufferColSize += calculateSizeOfColHelper(mapColumnVector.keys);
        converterBufferColSize += calculateSizeOfColHelper(mapColumnVector.values);
        break;
      case STRUCT:
        StructColumnVector structColumnVector = (StructColumnVector) col;
        for (int j = 0; j < structColumnVector.fields.length; j++) {
          converterBufferColSize += calculateSizeOfColHelper(structColumnVector.fields[j]);
        }
        break;
      case UNION:
        UnionColumnVector unionColumnVector = (UnionColumnVector) col;
        for (int j = 0; j < unionColumnVector.fields.length; j++) {
          converterBufferColSize += calculateSizeOfColHelper(unionColumnVector.fields[j]);
        }
        break;
      case BYTES:
        BytesColumnVector bytesColumnVector = (BytesColumnVector) col;
        converterBufferColSize += bytesColumnVector.vector.length * 8;
        break;
      case DECIMAL:
        DecimalColumnVector decimalColumnVector = (DecimalColumnVector) col;
        converterBufferColSize += decimalColumnVector.precision + 2;
        break;
      case DOUBLE:
        DoubleColumnVector doubleColumnVector = (DoubleColumnVector) col;
        converterBufferColSize += doubleColumnVector.vector.length * 8;
        break;
      case LONG:
        LongColumnVector longColumnVector = (LongColumnVector) col;
        converterBufferColSize += longColumnVector.vector.length * 8;
        break;
      default:
        // Should never reach here given the types used in GenericRecordToOrcValueWriter
    }
    // Calculate overhead of the column's own null reference
    converterBufferColSize += col.isNull.length;
    return converterBufferColSize;
  }

  /**
   * Returns the total size of all variable length columns in a {@link VectorizedRowBatch}
   * TODO: Consider calculating this value on the fly everytime a resize is called
   * @return
   */
  public long getConverterBufferTotalSize() {
    long converterBufferTotalSize = 0;
    ColumnVector[] cols = this.rowBatch.cols;
    for (int i = 0; i < cols.length; i++) {
      converterBufferTotalSize += calculateSizeOfColHelper(cols[i]);
    }
    return converterBufferTotalSize;
  }

  /**
   * Resize the child-array size based on configuration.
   * If smart resizing is enabled, it will use an exponential decay algorithm where it would resize the array by a smaller amount
   * the more records the converter has processed, as the fluctuation in record size becomes less likely to differ significantly by then
   * Since the writer is closed and reset periodically, this is generally a safe assumption that should prevent large empty array buffers
   */
  public int resize(int rowsAdded, int requestedSize) {
    resizeCount += 1;
    log.info(String.format("It has been resized %s times in current writer", resizeCount));
    if (enabledSmartSizing) {
      double decayingEnlargeFactor =  this.smartArrayEnlargeFactorMax * Math.pow((1-this.smartArrayEnlargeDecayFactor), rowsAdded-1);
      return (int) Math.round(requestedSize * Math.max(decayingEnlargeFactor, this.smartArrayEnlargeFactorMin));
    }
    return enlargeFactor * requestedSize;
  }
}
