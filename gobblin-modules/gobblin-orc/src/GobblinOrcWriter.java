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

import java.io.IOException;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.UnionColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.avro.AvroObjectInspectorGenerator;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

import com.google.common.annotations.VisibleForTesting;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.state.ConstructState;
import org.apache.gobblin.writer.FsDataWriter;
import org.apache.gobblin.writer.FsDataWriterBuilder;


/**
 * A wrapper for ORC-core writer without dependency on Hive SerDe library.
 */
@Slf4j
public class GobblinOrcWriter extends FsDataWriter<GenericRecord> {
  static final String ORC_WRITER_PREFIX = "orcWriter.";
  private static final String ORC_WRITER_BATCH_SIZE = ORC_WRITER_PREFIX + "batchSize";
  private static final int DEFAULT_ORC_WRITER_BATCH_SIZE = 1000;

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

  public GobblinOrcWriter(FsDataWriterBuilder<Schema, GenericRecord> builder, State properties)
      throws IOException {
    super(builder, properties);

    log.info("Start to construct a ORC-Native Writer");

    // Create value-writer which is essentially a record-by-record-converter with buffering in batch.
    this.avroSchema = builder.getSchema();
    TypeDescription typeDescription = AvroOrcSchemaConverter.getOrcSchema(this.avroSchema);
    this.valueWriter = new GenericRecordToOrcValueWriter(typeDescription, this.avroSchema, properties);
    this.batchSize = properties.getPropAsInt(ORC_WRITER_BATCH_SIZE, DEFAULT_ORC_WRITER_BATCH_SIZE);
    this.rowBatch = typeDescription.createRowBatch(this.batchSize);
    this.deepCleanBatch = properties.getPropAsBoolean(ORC_WRITER_DEEP_CLEAN_EVERY_BATCH, false);

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
      state.addOverwriteProperties(new State(getOrcSchemaAttrs(this.avroSchema.toString())));
    } catch (SerDeException | IOException e) {
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

  public static Properties getOrcSchemaAttrs(String avroSchemaString)
      throws SerDeException, IOException {
    Properties properties = new Properties();
    properties.setProperty(AvroSerdeUtils.AvroTableProperties.SCHEMA_LITERAL.getPropName(), avroSchemaString);

    AvroObjectInspectorGenerator aoig = new AvroObjectInspectorGenerator(
        AvroSerdeUtils.determineSchemaOrThrowException(properties));

    properties.setProperty("columns", StringUtils.join(aoig.getColumnNames(), ","));
    properties.setProperty("columns.types", StringUtils.join(aoig.getColumnTypes(), ","));

    return properties;
  }
}
