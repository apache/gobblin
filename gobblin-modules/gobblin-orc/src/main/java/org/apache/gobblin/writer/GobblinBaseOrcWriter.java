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
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;

import com.google.common.annotations.VisibleForTesting;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.state.ConstructState;

/**
 * A wrapper for ORC-core writer without dependency on Hive SerDe library.
 */
@Slf4j
public abstract class GobblinBaseOrcWriter<S, D> extends FsDataWriter<D> {
  static final String ORC_WRITER_PREFIX = "orcWriter.";
  public static final String ORC_WRITER_BATCH_SIZE = ORC_WRITER_PREFIX + "batchSize";
  public static final int DEFAULT_ORC_WRITER_BATCH_SIZE = 1000;

  private final OrcValueWriter<D> valueWriter;
  @VisibleForTesting
  VectorizedRowBatch rowBatch;
  private final TypeDescription typeDescription;
  private final Writer orcFileWriter;
  private final RowBatchPool rowBatchPool;
  private final boolean enableRowBatchPool;

  // the close method may be invoked multiple times, but the underlying writer only supports close being called once
  private volatile boolean closed = false;

  private final int batchSize;
  protected final S inputSchema;


  public GobblinBaseOrcWriter(FsDataWriterBuilder<S, D> builder, State properties)
      throws IOException {
    super(builder, properties);

    // Create value-writer which is essentially a record-by-record-converter with buffering in batch.
    this.inputSchema = builder.getSchema();
    this.typeDescription = getOrcSchema();
    this.valueWriter = getOrcValueWriter(typeDescription, this.inputSchema, properties);
    this.batchSize = properties.getPropAsInt(ORC_WRITER_BATCH_SIZE, DEFAULT_ORC_WRITER_BATCH_SIZE);
    this.rowBatchPool = RowBatchPool.instance(properties);
    this.enableRowBatchPool = properties.getPropAsBoolean(RowBatchPool.ENABLE_ROW_BATCH_POOL, false);
    this.rowBatch = enableRowBatchPool ? rowBatchPool.getRowBatch(typeDescription, batchSize) : typeDescription.createRowBatch(batchSize);
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
   * Get an {@link OrcValueWriter} for the specified schema and configuration.
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
    }
  }
}
