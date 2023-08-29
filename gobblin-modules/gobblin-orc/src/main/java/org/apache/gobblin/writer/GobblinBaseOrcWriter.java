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
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.state.ConstructState;

/**
 * A wrapper for ORC-core writer without dependency on Hive SerDe library.
 */
@Slf4j
public abstract class GobblinBaseOrcWriter<S, D> extends FsDataWriter<D> {
  public static final String ORC_WRITER_PREFIX = "orcWriter.";
  public static final String ORC_WRITER_BATCH_SIZE = ORC_WRITER_PREFIX + "batchSize";
  public static final int DEFAULT_ORC_WRITER_BATCH_SIZE = 1000;
  public static final String ORC_WRITER_AUTO_SELFTUNE_ENABLED = ORC_WRITER_PREFIX + "auto.selfTune.enabled";
  public static final String ORC_WRITER_ESTIMATED_RECORD_SIZE = ORC_WRITER_PREFIX + "estimated.recordSize";
  public static final String ORC_WRITER_AUTO_SELFTUNE_FREQUENCY = ORC_WRITER_PREFIX + "auto.selfTune.frequency";
  public static final String ORCWRITER_BATCHSIZE_MEMORY_USAGE_FACTOR = ORC_WRITER_PREFIX + "auto.selfTune.memory.usage.factor";
  public static final int DEFAULT_ORC_AUTO_SELFTUNE_ROWS_BETWEEN_CHECK = 500;
  public static final String ORC_WRITER_ESTIMATED_BYTES_ALLOCATED_CONVERTER_MEMORY = ORC_WRITER_PREFIX + "estimated.bytes.allocated.converter.memory";
   // This value gives an estimation on how many writers are buffering records at the same time in a container.
   // Since time-based partition scheme is a commonly used practice, plus the chances for late-arrival data,
   // usually there would be 2-3 writers running during the hourly boundary. 3 is chosen here for being conservative.
  private static final int CONCURRENT_WRITERS_DEFAULT = 3;
  public static final double DEFAULT_ORCWRITER_BATCHSIZE_MEMORY_USAGE_FACTOR = 0.5;
  public static final int DEFAULT_ORCWRITER_BATCHSIZE_ROWCHECK_FACTOR = 5;
  public static final int DEFAULT_MIN_ORCWRITER_ROWCHECK = 150;
  public static final int DEFAULT_MAX_ORCWRITER_ROWCHECK = 5000;
  // TODO: Generate this from ORC configurations
  public static final int DEFAULT_ORC_WRITER_STRIPE_SIZE = 250*1024*1000;
  public static final double DEFAULT_WRITER_AUTO_SELFTUNE_
  protected final OrcValueWriter<D> valueWriter;
  @VisibleForTesting
  VectorizedRowBatch rowBatch;
  private final TypeDescription typeDescription;
  protected Writer orcFileWriter;
  private final RowBatchPool rowBatchPool;
  private final boolean enableRowBatchPool;
  protected long estimatedRecordSizeBytes = -1;

  // the close method may be invoked multiple times, but the underlying writer only supports close being called once
  protected volatile boolean closed = false;

  protected int batchSize;
  protected final S inputSchema;

  private final boolean selfTuningWriter;
  private int selfTuneRowsBetweenCheck;
  private double batchSizeMemoryUsageFactor;
  private int nextSelfTune;
  private boolean initialEstimatingRecordSizePhase = false;
  private Queue<Integer> initialSelfTuneCheckpoints = new LinkedList<>(Arrays.asList(10, 100, 500));
  private AtomicInteger recordCounter = new AtomicInteger(0);
  private long availableMemory = -1;
  // Holds the maximum size of the previous run's maximum buffer or the max of the current run's maximum buffer
  private long estimatedBytesAllocatedConverterMemory = -1;
  private OrcConverterMemoryManager converterMemoryManager;
  // Used to ensure that when the fileWriter is in the process of being tuned it is not being used by another thread.
  private ReadWriteLock orcFileWriterLock = new ReentrantReadWriteLock();
  Configuration writerConfig;

  public GobblinBaseOrcWriter(FsDataWriterBuilder<S, D> builder, State properties)
      throws IOException {
    super(builder, properties);

    // Create value-writer which is essentially a record-by-record-converter with buffering in batch.
    this.inputSchema = builder.getSchema();
    this.typeDescription = getOrcSchema();
    this.valueWriter = getOrcValueWriter(typeDescription, this.inputSchema, properties);
    this.selfTuningWriter = properties.getPropAsBoolean(ORC_WRITER_AUTO_SELFTUNE_ENABLED, false);
    this.batchSize = this.selfTuningWriter ? DEFAULT_ORC_WRITER_BATCH_SIZE : properties.getPropAsInt(ORC_WRITER_BATCH_SIZE, DEFAULT_ORC_WRITER_BATCH_SIZE);
    this.rowBatchPool = RowBatchPool.instance(properties);
    this.enableRowBatchPool = properties.getPropAsBoolean(RowBatchPool.ENABLE_ROW_BATCH_POOL, false);
    this.selfTuneRowsBetweenCheck = properties.getPropAsInt(ORC_WRITER_AUTO_SELFTUNE_FREQUENCY, DEFAULT_ORC_AUTO_SELFTUNE_ROWS_BETWEEN_CHECK);
    this.batchSizeMemoryUsageFactor = properties.getPropAsDouble(ORCWRITER_BATCHSIZE_MEMORY_USAGE_FACTOR, DEFAULT_ORCWRITER_BATCHSIZE_MEMORY_USAGE_FACTOR);
    this.rowBatch = enableRowBatchPool ? rowBatchPool.getRowBatch(typeDescription, batchSize) : typeDescription.createRowBatch(batchSize);
    this.converterMemoryManager = new OrcConverterMemoryManager(this.rowBatch);
    // Create file-writer
    this.writerConfig = new Configuration();
    // Populate job Configurations into Conf as well so that configurations related to ORC writer can be tuned easily.
    for (Object key : properties.getProperties().keySet()) {
      this.writerConfig.set((String) key, properties.getProp((String) key));
    }
    OrcFile.WriterOptions options = OrcFile.writerOptions(properties.getProperties(), this.writerConfig);
    options.setSchema(typeDescription);

    // Get the amount of allocated and future space available
    this.availableMemory = Runtime.getRuntime().maxMemory() - (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory());
    log.info("Available memory for ORC writer: {}", this.availableMemory);

    if (this.selfTuningWriter) {
      if (properties.contains(ORC_WRITER_ESTIMATED_RECORD_SIZE)) {
        this.estimatedRecordSizeBytes = properties.getPropAsLong(ORC_WRITER_ESTIMATED_RECORD_SIZE);
        this.estimatedBytesAllocatedConverterMemory = properties.getPropAsLong(ORC_WRITER_ESTIMATED_BYTES_ALLOCATED_CONVERTER_MEMORY, -1);
        // Use the last run's rows between check value for the underlying file size writer, if it exists. Otherwise it will default to 5000
        log.info("Using previously stored properties to calculate new batch size, ORC Estimated Record size is : {}, e"
                + "estimated bytes converter allocated is : {}, ORC rows between check is {}",
            this.estimatedRecordSizeBytes, this.estimatedBytesAllocatedConverterMemory, properties.getPropAsInt(OrcConf.ROWS_BETWEEN_CHECKS.getAttribute()));
        this.tuneBatchSize(estimatedRecordSizeBytes, properties.getPropAsInt(OrcConf.ROWS_BETWEEN_CHECKS.getAttribute()));
        this.nextSelfTune = this.selfTuneRowsBetweenCheck;
      } else {
        // We will need to incrementally tune the writer based on the first few records
        this.nextSelfTune = 5;
        this.initialEstimatingRecordSizePhase = true;
      }
    } else {
      this.batchSize = properties.getPropAsInt(ORC_WRITER_BATCH_SIZE, DEFAULT_ORC_WRITER_BATCH_SIZE);
      log.info("Created ORC writer, batch size: {}, {}: {}",
          this.batchSize, OrcConf.ROWS_BETWEEN_CHECKS.getAttribute(),
          this.writerConfig.get(
              OrcConf.ROWS_BETWEEN_CHECKS.getAttribute(),
              OrcConf.ROWS_BETWEEN_CHECKS.getDefaultValue().toString()));
      this.orcFileWriter = OrcFile.createWriter(this.stagingFile, options);
    }
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
    return this.orcFileWriter != null ? this.orcFileWriter.getNumberOfRows(): 0;
  }

  @Override
  public long bytesWritten() {
    return this.orcFileWriter != null ? this.orcFileWriter.getRawDataSize() : 0;
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

  protected void recycleRowBatchPool() {
    if (enableRowBatchPool) {
      rowBatchPool.recycle(typeDescription, rowBatch);
    }
  }

  protected synchronized void closeInternal()
      throws IOException {
    if (!closed) {
      this.flush();
      this.orcFileWriter.close();
      this.closed = true;
      this.recycleRowBatchPool();
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
    if (this.selfTuningWriter) {
      properties.setProp(ORC_WRITER_ESTIMATED_RECORD_SIZE, String.valueOf(estimatedRecordSizeBytes));
      properties.setProp(ORC_WRITER_ESTIMATED_BYTES_ALLOCATED_CONVERTER_MEMORY, String.valueOf(this.converterMemoryManager.getConverterBufferTotalSize()));
      properties.setProp(OrcConf.ROWS_BETWEEN_CHECKS.getAttribute(), String.valueOf(this.batchSize * calculateOrcFileWriterRowsBetweenCheck()));
    }
  }

  /**
   * Modifies the size of the writer buffer based on the average size of the records written so far, the amount of available memory during initialization, and the number of concurrent writers.
   * The new batch size is calculated as follows:
   * 1. Memory available = (available memory during startup)/(concurrent writers) - (memory used by ORCFile writer)
   * 2. Average file size, estimated during Avro -> ORC conversion
   * 3. Estimate of memory used by the converter lists, as during resize the internal buffer size can grow large
   * 4. New batch size = (Memory available - Estimated memory used by converter lists) / Average file size * memory buffer
   * Generally in this writer, the memory the converter uses for large arrays is the leading cause of OOM in streaming, along with the records stored in the rowBatch
   * Another potential approach is to also check the memory available before resizing the converter lists, and to flush the batch whenever a resize is needed.
   */
  void tuneBatchSize(long averageSizePerRecord, int orcFileWriterRowsBetweenCheck) throws IOException {
    this.estimatedBytesAllocatedConverterMemory = Math.max(this.estimatedBytesAllocatedConverterMemory, this.converterMemoryManager.getConverterBufferTotalSize());
    int currentConcurrentWriters = this.properties.getPropAsInt(PartitionedDataWriter.CURRENT_PARTITIONED_WRITERS_COUNTER, CONCURRENT_WRITERS_DEFAULT);
    // In the native ORC writer implementation, it will flush the writer if the internal memory exceeds the size of a stripe after rows between check
    // So worst case the most memory the writer can hold is the size of a stripe plus size of records * number of records between checks
    // Note that this is an overestimate as the native ORC file writer should have some compression ratio
    long maxMemoryInFileWriter = this.estimatedRecordSizeBytes * orcFileWriterRowsBetweenCheck + DEFAULT_ORC_WRITER_STRIPE_SIZE;

    int newBatchSize = (int) Math.round(((this.availableMemory/currentConcurrentWriters - maxMemoryInFileWriter
        - this.estimatedBytesAllocatedConverterMemory) * this.batchSizeMemoryUsageFactor) / averageSizePerRecord);
    // Handle scenarios where new batch size can be 0 or less due to overestimating memory used by other components
    newBatchSize = Math.min(Math.max(1, newBatchSize), DEFAULT_ORC_WRITER_BATCH_SIZE);
    // TODO: Consider using a more sophisticated check to determine if the batch size should be changed
    if (Math.abs(newBatchSize - this.batchSize) > 0.2 * this.batchSize) {
      log.info("Tuning ORC writer batch size from {} to {} based on average byte size per record: {} with available memory {} and {} bytes of allocated memory in converter buffers, with {} concurrent writers",
          batchSize, newBatchSize, averageSizePerRecord, availableMemory, estimatedBytesAllocatedConverterMemory, currentConcurrentWriters);
      this.batchSize = newBatchSize;
      // We only initialize the native ORC file writer once to avoid creating too many small files, as reconfiguring rows between memory check
      // requires one to close the writer and start a new file
      if (this.orcFileWriter == null) {
        initializeOrcFileWriter();
      }
      this.flush();
      this.rowBatch.ensureSize(this.batchSize);
    }
    if (this.orcFileWriter == null) {
      initializeOrcFileWriter();
    }
  }

  void initializeOrcFileWriter() {
    try {
      int rowBetweenCheck = calculateOrcFileWriterRowsBetweenCheck();
      this.writerConfig.set(OrcConf.ROWS_BETWEEN_CHECKS.getAttribute(), String.valueOf(rowBetweenCheck));
      log.info("Created ORC writer, batch size: {}, {}: {}",
          this.batchSize, OrcConf.ROWS_BETWEEN_CHECKS.getAttribute(),
          this.writerConfig.get(
              OrcConf.ROWS_BETWEEN_CHECKS.getAttribute(),
              OrcConf.ROWS_BETWEEN_CHECKS.getDefaultValue().toString()));
      OrcFile.WriterOptions options = OrcFile.writerOptions(properties.getProperties(), this.writerConfig);
      options.setSchema(typeDescription);
      this.orcFileWriter = OrcFile.createWriter(this.stagingFile, options);
    } catch (IOException e) {
      log.error("Failed to flush the current batch", e);
    }
  }

  int calculateOrcFileWriterRowsBetweenCheck() {
    return Math.max(Math.min(this.batchSize * DEFAULT_ORCWRITER_BATCHSIZE_ROWCHECK_FACTOR, DEFAULT_MAX_ORCWRITER_ROWCHECK), DEFAULT_MIN_ORCWRITER_ROWCHECK);
  }

  /*
   * Note: orc.rows.between.memory.checks is the configuration available to tune memory-check sensitivity in ORC-Core
   * library. By default it is set to 5000. If the user-application is dealing with large-row Kafka topics for example,
   * one should consider lower this value to make memory-check more active.
   */
  @Override
  public void write(D record)
      throws IOException {
    Preconditions.checkState(!closed, "Writer already closed");
    this.valueWriter.write(record, this.rowBatch);
    int recordCount = this.recordCounter.incrementAndGet();
    if (this.selfTuningWriter && recordCount == this.nextSelfTune) {
      long totalBytes = ((GenericRecordToOrcValueWriter) valueWriter).getTotalBytesConverted();
      long totalRecords = ((GenericRecordToOrcValueWriter) valueWriter).getTotalRecordsConverted();
      this.estimatedRecordSizeBytes = totalRecords == 0 ? 0 : totalBytes / totalRecords;
      this.tuneBatchSize(this.estimatedRecordSizeBytes, this.calculateOrcFileWriterRowsBetweenCheck());
      if (this.initialEstimatingRecordSizePhase && !initialSelfTuneCheckpoints.isEmpty()) {
        this.nextSelfTune = initialSelfTuneCheckpoints.poll();
      } else {
        this.nextSelfTune += this.selfTuneRowsBetweenCheck;
      }
    }
    if (rowBatch.size == this.batchSize) {
      this.flush();
    }
  }
}
