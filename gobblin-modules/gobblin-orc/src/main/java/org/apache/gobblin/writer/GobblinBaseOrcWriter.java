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
import java.util.Properties;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
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
import org.apache.gobblin.state.ConstructState;

import static org.apache.gobblin.configuration.ConfigurationKeys.AVG_RECORD_SIZE;

/**
 * A wrapper for ORC-core writer without dependency on Hive SerDe library.
 */
@Slf4j
public abstract class GobblinBaseOrcWriter<S, D> extends FsDataWriter<D> {
  static final String ORC_WRITER_PREFIX = "orcWriter.";
  public static final String ORC_WRITER_BATCH_SIZE = ORC_WRITER_PREFIX + "batchSize";
  public static final int DEFAULT_ORC_WRITER_BATCH_SIZE = 1000;

  private static final String CONTAINER_MEMORY_MBS = ORC_WRITER_PREFIX + "jvm.memory.mbs";
  private static final int DEFAULT_CONTAINER_MEMORY_MBS = 4096;

  private static final String CONTAINER_JVM_MEMORY_XMX_RATIO_KEY = ORC_WRITER_PREFIX + "container.jvmMemoryXmxRatio";
  private static final double DEFAULT_CONTAINER_JVM_MEMORY_XMX_RATIO_KEY = 1.0;

  static final String CONTAINER_JVM_MEMORY_OVERHEAD_MBS = ORC_WRITER_PREFIX + "container.jvmMemoryOverheadMbs";
  private static final int DEFAULT_CONTAINER_JVM_MEMORY_OVERHEAD_MBS = 0;

  @VisibleForTesting
  static final String ORC_WRITER_AUTO_TUNE_ENABLED = ORC_WRITER_PREFIX + "autoTuneEnabled";
  private static final boolean ORC_WRITER_AUTO_TUNE_DEFAULT = false;
  private static final long EXEMPLIFIED_RECORD_SIZE_IN_BYTES = 1024;

  /**
   * This value gives an estimation on how many writers are buffering records at the same time in a container.
   * Since time-based partition scheme is a commonly used practice, plus the chances for late-arrival data,
   * usually there would be 2-3 writers running during the hourly boundary. 3 is chosen here for being conservative.
   */
  private static final int ESTIMATED_PARALLELISM_WRITERS = 3;

  // The serialized record size passed from AVG_RECORD_SIZE is smaller than the actual in-memory representation
  // of a record. This is just the number represents how many times that the actual buffer storing record is larger
  // than the serialized size passed down from upstream constructs.
  @VisibleForTesting
  static final String RECORD_SIZE_SCALE_FACTOR = "recordSize.scaleFactor";
  static final int DEFAULT_RECORD_SIZE_SCALE_FACTOR = 6;

  /**
   * Check comment of {@link #deepCleanRowBatch} for the usage of this configuration.
   */
  private static final String ORC_WRITER_DEEP_CLEAN_EVERY_BATCH = ORC_WRITER_PREFIX + "deepCleanBatch";

  private final OrcValueWriter<D> valueWriter;
  @VisibleForTesting
  VectorizedRowBatch rowBatch;
  private final TypeDescription typeDescription;
  private final Writer orcFileWriter;
  private final RowBatchPool rowBatchPool;
  private final boolean enableRowBatchPool;

  // the close method may be invoked multiple times, but the underlying writer only supports close being called once
  private volatile boolean closed = false;
  private final boolean deepCleanBatch;

  private final int batchSize;
  protected final S inputSchema;

  /**
   * There are a couple of parameters in ORC writer that requires manual tuning based on record size given that executor
   * for running these ORC writers has limited heap space. This helper function wrap them and has side effect for the
   * argument {@param properties}.
   *
   * Assumption for current implementation:
   * The extractor or source class should set {@link org.apache.gobblin.configuration.ConfigurationKeys#AVG_RECORD_SIZE}
   */
  protected void autoTunedOrcWriterParams(State properties) {
    double writerRatio = properties.getPropAsDouble(OrcConf.MEMORY_POOL.name(), (double) OrcConf.MEMORY_POOL.getDefaultValue());
    long availableHeapPerWriter = Math.round(availableHeapSize(properties) * writerRatio / ESTIMATED_PARALLELISM_WRITERS);

    // Upstream constructs will need to set this value properly
    long estimatedRecordSize = getEstimatedRecordSize(properties);
    long rowsBetweenCheck = availableHeapPerWriter * 1024 / estimatedRecordSize;
    properties.setProp(OrcConf.ROWS_BETWEEN_CHECKS.name(),
        Math.min(rowsBetweenCheck, (int) OrcConf.ROWS_BETWEEN_CHECKS.getDefaultValue()));
    // Row batch size should be smaller than row_between_check, 4 is just a magic number picked here.
    long batchSize = Math.min(rowsBetweenCheck / 4, DEFAULT_ORC_WRITER_BATCH_SIZE);
    properties.setProp(ORC_WRITER_BATCH_SIZE, batchSize);
    log.info("Tuned the parameter " + OrcConf.ROWS_BETWEEN_CHECKS.name() + " to be:" + rowsBetweenCheck + ","
        + ORC_WRITER_BATCH_SIZE + " to be:" + batchSize);
  }

  /**
   * Calculate the heap size in MB available for ORC writers.
   */
  protected long availableHeapSize(State properties) {
    // Calculate the recommended size as the threshold for memory check
    long physicalMem = Math.round(properties.getPropAsLong(CONTAINER_MEMORY_MBS, DEFAULT_CONTAINER_MEMORY_MBS)
        * properties.getPropAsDouble(CONTAINER_JVM_MEMORY_XMX_RATIO_KEY, DEFAULT_CONTAINER_JVM_MEMORY_XMX_RATIO_KEY));
    long nonHeap = properties.getPropAsLong(CONTAINER_JVM_MEMORY_OVERHEAD_MBS, DEFAULT_CONTAINER_JVM_MEMORY_OVERHEAD_MBS);
    return physicalMem - nonHeap;
  }

  /**
   * Calculate the estimated record size in bytes.
   */
  protected long getEstimatedRecordSize(State properties) {
    long estimatedRecordSizeScale = properties.getPropAsInt(RECORD_SIZE_SCALE_FACTOR, DEFAULT_RECORD_SIZE_SCALE_FACTOR);
    return (properties.contains(AVG_RECORD_SIZE) ? properties.getPropAsLong(AVG_RECORD_SIZE)
        : EXEMPLIFIED_RECORD_SIZE_IN_BYTES) * estimatedRecordSizeScale;
  }

  public GobblinBaseOrcWriter(FsDataWriterBuilder<S, D> builder, State properties)
      throws IOException {
    super(builder, properties);
    if (properties.getPropAsBoolean(ORC_WRITER_AUTO_TUNE_ENABLED, ORC_WRITER_AUTO_TUNE_DEFAULT)) {
      autoTunedOrcWriterParams(properties);
    }

    // Create value-writer which is essentially a record-by-record-converter with buffering in batch.
    this.inputSchema = builder.getSchema();
    this.typeDescription = getOrcSchema();
    this.valueWriter = getOrcValueWriter(typeDescription, this.inputSchema, properties);
    this.batchSize = properties.getPropAsInt(ORC_WRITER_BATCH_SIZE, DEFAULT_ORC_WRITER_BATCH_SIZE);
    this.rowBatchPool = RowBatchPool.instance(properties);
    this.enableRowBatchPool = properties.getPropAsBoolean(RowBatchPool.ENABLE_ROW_BATCH_POOL, false);
    this.rowBatch = enableRowBatchPool ? rowBatchPool.getRowBatch(typeDescription, batchSize) : typeDescription.createRowBatch(batchSize);
    this.deepCleanBatch = properties.getPropAsBoolean(ORC_WRITER_DEEP_CLEAN_EVERY_BATCH, false);

    log.info("Created ORC writer, batch size: {}, {}: {}",
            batchSize, OrcConf.ROWS_BETWEEN_CHECKS.getAttribute(),
            properties.getProp(
                    OrcConf.ROWS_BETWEEN_CHECKS.getAttribute(),
                    OrcConf.ROWS_BETWEEN_CHECKS.getDefaultValue().toString()));

    // Create file-writer
    Configuration conf = new Configuration();
    // Populate job Configurations into Conf as well so that configurations related to ORC writer can be tuned easily.
    for (Object key : properties.getProperties().keySet()) {
      conf.set((String) key, properties.getProp((String) key));
    }

    OrcFile.WriterOptions options = OrcFile.writerOptions(properties.getProperties(), conf);
    options.setSchema(typeDescription);

    // For buffer-writer, flush has to be executed before close so it is better we maintain the life-cycle of fileWriter
    // instead of delegating it to closer object in FsDataWriter.
    this.orcFileWriter = OrcFile.createWriter(this.stagingFile, options);
  }

  /**
   * Get the ORC schema as a {@link TypeDescription}
   */
  protected abstract TypeDescription getOrcSchema();

  /**
   * Get an {@OrcValueWriter} for the specified schema and configuration.
   */
  protected abstract OrcValueWriter<D> getOrcValueWriter(TypeDescription typeDescription, S inputSchema, State state);

  /**
   * Get the schema properties, including the following:
   * avro.schema.literal
   * columns
   * column_types
   */
  protected abstract Properties getPropsWithOrcSchema() throws SerDeException;

  @Override
  public long recordsWritten() {
    return this.orcFileWriter.getNumberOfRows();
  }

  @Override
  public State getFinalState() {
    /**
     * Creating {@link ConstructState} to provide overwrite of {@link WorkUnitState} from constructs.
     */
    ConstructState state = new ConstructState(super.getFinalState());
    try {
      state.addOverwriteProperties(new State(getPropsWithOrcSchema()));
    } catch (SerDeException e) {
      throw new RuntimeException("Failure to set schema metadata in finalState properly which "
          + "could possible lead to incorrect data registration", e);
    }

    return state;
  }

  @Override
  public void flush()
      throws IOException {
    if (rowBatch.size > 0) {
      orcFileWriter.addRowBatch(rowBatch);
      rowBatch.reset();
      if (deepCleanBatch) {
        deepCleanRowBatch(rowBatch);
      }
    }
  }

  private synchronized void closeInternal()
      throws IOException {
    if (!closed) {
      this.flush();
      this.orcFileWriter.close();
      this.closed = true;
      if (enableRowBatchPool) {
        rowBatchPool.recycle(typeDescription, rowBatch);
      }
    } else {
      // Throw fatal exception if there's outstanding buffered data since there's risk losing data if proceeds.
      if (rowBatch.size > 0) {
        throw new CloseBeforeFlushException(this.inputSchema.toString());
      }
    }
  }

  @Override
  public void close()
      throws IOException {
    closeInternal();
    super.close();
  }

  /**
   * Extra careful about the fact: super.commit() invoke closer.close based on its semantics of "commit".
   * That means close can happen in both commit() and close()
   */
  @Override
  public void commit()
      throws IOException {
    closeInternal();
    super.commit();
  }

  /**
   * Note: orc.rows.between.memory.checks is the configuration available to tune memory-check sensitivity in ORC-Core
   * library. By default it is set to 5000. If the user-application is dealing with large-row Kafka topics for example,
   * one should consider lower this value to make memory-check more active.
   */
  @Override
  public void write(D record)
      throws IOException {
    Preconditions.checkState(!closed, "Writer already closed");
    valueWriter.write(record, rowBatch);
    if (rowBatch.size == this.batchSize) {
      orcFileWriter.addRowBatch(rowBatch);
      rowBatch.reset();
      if (deepCleanBatch) {
        log.info("A reset of rowBatch is triggered - releasing holding memory for large object");
        deepCleanRowBatch(rowBatch);
      }
    }
  }

  /**
   * The reset call of {@link VectorizedRowBatch} doesn't release the memory occupied by each {@link ColumnVector}'s child,
   * which is usually an array of objects, while it only set those value to null.
   * This method ensure the reference to the child array for {@link ColumnVector} are released and gives a hint of GC,
   * so that each reset could release the memory pre-allocated by {@link ColumnVector#ensureSize(int, boolean)} method.
   *
   * This feature is configurable and should only be turned on if a dataset is:
   * 1. Has large per-record size.
   * 2. Has {@link org.apache.hadoop.hive.ql.exec.vector.MultiValuedColumnVector} as part of schema,
   * like array, map and all nested structures containing these.
   */
  @VisibleForTesting
  void deepCleanRowBatch(VectorizedRowBatch rowBatch) {
    for(int i = 0; i < rowBatch.cols.length; ++i) {
      ColumnVector cv = rowBatch.cols[i];
      if (cv != null) {
        removeRefOfColumnVectorChild(cv);
      }
    }
  }

  /**
   * Set the child field of {@link ColumnVector} to null, assuming input {@link ColumnVector} is nonNull.
   */
  private void removeRefOfColumnVectorChild(ColumnVector cv) {
    if (cv instanceof StructColumnVector) {
      StructColumnVector structCv = (StructColumnVector) cv;
      for (ColumnVector childCv: structCv.fields) {
        removeRefOfColumnVectorChild(childCv);
      }
    } else if (cv instanceof ListColumnVector) {
      ListColumnVector listCv = (ListColumnVector) cv;
      removeRefOfColumnVectorChild(listCv.child);
    } else if (cv instanceof MapColumnVector) {
      MapColumnVector mapCv = (MapColumnVector) cv;
      removeRefOfColumnVectorChild(mapCv.keys);
      removeRefOfColumnVectorChild(mapCv.values);
    } else if (cv instanceof UnionColumnVector) {
      UnionColumnVector unionCv = (UnionColumnVector) cv;
      for (ColumnVector unionChildCv : unionCv.fields) {
        removeRefOfColumnVectorChild(unionChildCv);
      }
    } else if (cv instanceof LongColumnVector) {
      ((LongColumnVector) cv).vector = null;
    } else if (cv instanceof DoubleColumnVector) {
      ((DoubleColumnVector) cv).vector = null;
    } else if (cv instanceof BytesColumnVector) {
      ((BytesColumnVector) cv).vector = null;
      ((BytesColumnVector) cv).start = null;
      ((BytesColumnVector) cv).length = null;
    } else if (cv instanceof DecimalColumnVector) {
      ((DecimalColumnVector) cv).vector = null;
    }
  }
}
