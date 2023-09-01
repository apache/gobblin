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


/**
 * A helper class to calculate the size of array buffers in a {@link VectorizedRowBatch}.
 * This estimate is mainly based on the maximum size of each variable length column, which can be resized
 * Since the resizing algorithm for each column can balloon, this can affect likelihood of OOM
 */
public class OrcConverterMemoryManager {

  private VectorizedRowBatch rowBatch;

  // TODO: Consider moving the resize algorithm from the converter to this class
  OrcConverterMemoryManager(VectorizedRowBatch rowBatch) {
    this.rowBatch = rowBatch;
  }

  /**
   * Estimates the approximate size in bytes of elements in a column
   * This takes into account the default null values of different ORC ColumnVectors and approximates their sizes
   * Although its a rough calculation, it can accurately depict the weight of resizes in a column for very large arrays and maps
   * Which tend to dominate the size of the buffer overall
   * TODO: Clean this method up considerably and refactor resize logic here
   * @param col
   * @return
   */
  public long calculateSizeOfColHelper(ColumnVector col) {
    long converterBufferColSize = 0;
    if (col instanceof ListColumnVector) {
      ListColumnVector listColumnVector = (ListColumnVector) col;
      converterBufferColSize += calculateSizeOfColHelper(listColumnVector.child);
    } else if (col instanceof MapColumnVector) {
      MapColumnVector mapColumnVector = (MapColumnVector) col;
      converterBufferColSize += calculateSizeOfColHelper(mapColumnVector.keys);
      converterBufferColSize += calculateSizeOfColHelper(mapColumnVector.values);
    } else if (col instanceof StructColumnVector) {
      StructColumnVector structColumnVector = (StructColumnVector) col;
      for (int j = 0; j < structColumnVector.fields.length; j++) {
        converterBufferColSize += calculateSizeOfColHelper(structColumnVector.fields[j]);
      }
    } else if (col instanceof UnionColumnVector) {
      UnionColumnVector unionColumnVector = (UnionColumnVector) col;
      for (int j = 0; j < unionColumnVector.fields.length; j++) {
        converterBufferColSize += calculateSizeOfColHelper(unionColumnVector.fields[j]);
      }
    } else if (col instanceof LongColumnVector || col instanceof DoubleColumnVector || col instanceof DecimalColumnVector) {
      // Memory space in bytes of native type
      converterBufferColSize += col.isNull.length * 8;
    } else if (col instanceof BytesColumnVector) {
      // Contains two integer list references of size vector for tracking so will use that as null size
      converterBufferColSize += ((BytesColumnVector) col).vector.length * 8;
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

}
