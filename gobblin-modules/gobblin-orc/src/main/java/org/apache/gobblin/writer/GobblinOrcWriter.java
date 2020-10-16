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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.avro.AvroObjectInspectorGenerator;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils;
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
import com.google.common.base.Preconditions;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.state.ConstructState;

import static org.apache.gobblin.source.extractor.extract.kafka.KafkaSource.AVG_RECORD_SIZE;
import static org.apache.gobblin.writer.AvroOrcSchemaConverter.getOrcSchema;
import static org.apache.gobblin.yarn.GobblinYarnConfigurationKeys.*;


/**
 * A wrapper for ORC-core writer without dependency on Hive SerDe library.
 */
@Slf4j
public class GobblinOrcWriter extends FsDataWriter<GenericRecord> {
  static final String ORC_WRITER_PREFIX = "orcWriter.";
  private static final String ORC_WRITER_BATCH_SIZE = ORC_WRITER_PREFIX + "batchSize";
  private static final int DEFAULT_ORC_WRITER_BATCH_SIZE = 1000;
  @VisibleForTesting
  static final String ORC_WRITER_AUTO_TUNE_ENABLED = ORC_WRITER_PREFIX + "autoTuneEnabled";
  private static final boolean ORC_WRITER_AUTO_TUNE_DEFAULT = false;
  private static final long EXEMPLIFIED_RECORD_SIZE_IN_BYTES = 1024;
  private static final int PARALLELISM_WRITERS = 3;

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

  private final GenericRecordToOrcValueWriter valueWriter;
  @VisibleForTesting
  final VectorizedRowBatch rowBatch;
  private final Writer orcFileWriter;

  // the close method may be invoked multiple times, but the underlying writer only supports close being called once
  private volatile boolean closed = false;
  private final boolean deepCleanBatch;

  private final int batchSize;
  private final Schema avroSchema;

  /**
   * There are couple of parameters in ORC writer that requires manual tuning based on record size given that executor
   * for running these ORC writers has limited heap space. This helper function wrap them and has side effect for the
   * argument {@param properties}.
   *
   * Assumption for current implementation:
   * - Running in Gobblin-on-YARN mode to enable this feature as all the memory settings here are
   * relevant to the resources requested from YARN.
   * - Record size is provided by AVG_RECORD_SIZE which is a kafka related attribute. For other sources, upstream
   * constructs should provide its representation of record size.
   *
   * One should overwrite the behavior if plugging into systems where assumptions above don't hold.
   */
  protected void autoTunedOrcWriterParams(State properties) {
    double writerRatio = properties.getPropAsDouble(OrcConf.MEMORY_POOL.name(), (double) OrcConf.MEMORY_POOL.getDefaultValue());
    long availableHeapPerWriter = Math.round(availableHeapSize(properties) * writerRatio / PARALLELISM_WRITERS);

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
  protected long availableHeapSize(State Properties) {
    // Calculate the recommended size as the threshold for memory check
    long physicalMem = Math.round(Properties.getPropAsLong(CONTAINER_MEMORY_MBS_KEY, 4096)
        * properties.getPropAsDouble(CONTAINER_JVM_MEMORY_XMX_RATIO_KEY, DEFAULT_CONTAINER_JVM_MEMORY_XMX_RATIO));
    long nonHeap = properties.getPropAsLong(CONTAINER_JVM_MEMORY_OVERHEAD_MBS_KEY, DEFAULT_CONTAINER_JVM_MEMORY_OVERHEAD_MBS);
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

  public GobblinOrcWriter(FsDataWriterBuilder<Schema, GenericRecord> builder, State properties)
      throws IOException {
    super(builder, properties);
    if (properties.getPropAsBoolean(ORC_WRITER_AUTO_TUNE_ENABLED, ORC_WRITER_AUTO_TUNE_DEFAULT)) {
      autoTunedOrcWriterParams(properties);
    }

    // Create value-writer which is essentially a record-by-record-converter with buffering in batch.
    this.avroSchema = builder.getSchema();
    TypeDescription typeDescription = getOrcSchema(this.avroSchema);
    this.valueWriter = new GenericRecordToOrcValueWriter(typeDescription, this.avroSchema, properties);
    this.batchSize = properties.getPropAsInt(ORC_WRITER_BATCH_SIZE, DEFAULT_ORC_WRITER_BATCH_SIZE);
    this.rowBatch = typeDescription.createRowBatch(this.batchSize);
    this.deepCleanBatch = properties.getPropAsBoolean(ORC_WRITER_DEEP_CLEAN_EVERY_BATCH, false);

    log.info("Start to construct a ORC-Native Writer, with batchSize:" + batchSize + ", enable batchDeepClean:"
        + deepCleanBatch + "\n, schema in avro format:" + this.avroSchema);

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
      state.addOverwriteProperties(new State(getPropsWithOrcSchema(this.avroSchema)));
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
    } else {
      // Throw fatal exception if there's outstanding buffered data since there's risk losing data if proceeds.
      if (rowBatch.size > 0) {
        throw new CloseBeforeFlushException(this.avroSchema.getName());
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
  public void write(GenericRecord record)
      throws IOException {
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

  @Override
  public boolean isSpeculativeAttemptSafe() {
    return this.writerAttemptIdOptional.isPresent() && this.getClass() == GobblinOrcWriter.class;
  }

  public static Properties getPropsWithOrcSchema(Schema avroSchema) throws SerDeException {
    Properties properties = new Properties();
    properties.setProperty(AvroSerdeUtils.AvroTableProperties.SCHEMA_LITERAL.getPropName(), avroSchema.toString());
    AvroObjectInspectorGenerator aoig = new AvroObjectInspectorGenerator(avroSchema);

    properties.setProperty("columns", StringUtils.join(aoig.getColumnNames(), ","));
    properties.setProperty("columns.types", StringUtils.join(aoig.getColumnTypes(), ","));

    return properties;
  }
}
