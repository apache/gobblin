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
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.reflect.ConstructorUtils;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.io.Closer;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.commit.SpeculativeAttemptAwareConstruct;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.instrumented.writer.InstrumentedDataWriterDecorator;
import org.apache.gobblin.instrumented.writer.InstrumentedPartitionedDataWriterDecorator;
import org.apache.gobblin.records.ControlMessageHandler;
import org.apache.gobblin.source.extractor.CheckpointableWatermark;
import org.apache.gobblin.stream.ControlMessage;
import org.apache.gobblin.stream.MetadataUpdateControlMessage;
import org.apache.gobblin.stream.RecordEnvelope;
import org.apache.gobblin.stream.StreamEntity;
import org.apache.gobblin.util.AvroUtils;
import org.apache.gobblin.util.FinalState;
import org.apache.gobblin.writer.partitioner.WriterPartitioner;


/**
 * {@link DataWriter} that partitions data using a partitioner, instantiates appropriate writers, and sends records to
 * the chosen writer.
 * @param <S> schema type.
 * @param <D> record type.
 */
@Slf4j
public class PartitionedDataWriter<S, D> extends WriterWrapper<D> implements FinalState, SpeculativeAttemptAwareConstruct, WatermarkAwareWriter<D> {

  private static final GenericRecord NON_PARTITIONED_WRITER_KEY =
      new GenericData.Record(SchemaBuilder.record("Dummy").fields().endRecord());

  private int writerIdSuffix = 0;
  private final String baseWriterId;
  private final Optional<WriterPartitioner> partitioner;
  private final LoadingCache<GenericRecord, DataWriter<D>> partitionWriters;
  private final Optional<PartitionAwareDataWriterBuilder> builder;
  private final DataWriterBuilder writerBuilder;
  private final boolean shouldPartition;
  private final Closer closer;
  private final ControlMessageHandler controlMessageHandler;
  private boolean isSpeculativeAttemptSafe;
  private boolean isWatermarkCapable;

  public PartitionedDataWriter(DataWriterBuilder<S, D> builder, final State state)
      throws IOException {
    this.isSpeculativeAttemptSafe = true;
    this.isWatermarkCapable = true;
    this.baseWriterId = builder.getWriterId();
    this.closer = Closer.create();
    this.writerBuilder = builder;
    this.controlMessageHandler = new PartitionDataWriterMessageHandler();
    this.partitionWriters = CacheBuilder.newBuilder().build(new CacheLoader<GenericRecord, DataWriter<D>>() {
      @Override
      public DataWriter<D> load(final GenericRecord key)
          throws Exception {
        /* wrap the data writer to allow the option to close the writer on flush */
        return PartitionedDataWriter.this.closer
            .register(new InstrumentedPartitionedDataWriterDecorator<>(
                new CloseOnFlushWriterWrapper<D>(new Supplier<DataWriter<D>>() {
                  @Override
                  public DataWriter<D> get() {
                    try {
                      return createPartitionWriter(key);
                    } catch (IOException e) {
                      throw new RuntimeException("Error creating writer", e);
                    }
                  }
                }, state), state, key));
      }
    });

    if (state.contains(ConfigurationKeys.WRITER_PARTITIONER_CLASS)) {
      Preconditions.checkArgument(builder instanceof PartitionAwareDataWriterBuilder, String
              .format("%s was specified but the writer %s does not support partitioning.",
                  ConfigurationKeys.WRITER_PARTITIONER_CLASS, builder.getClass().getCanonicalName()));

      try {
        this.shouldPartition = true;
        this.builder = Optional.of(PartitionAwareDataWriterBuilder.class.cast(builder));
        this.partitioner = Optional.of(WriterPartitioner.class.cast(ConstructorUtils
                .invokeConstructor(Class.forName(state.getProp(ConfigurationKeys.WRITER_PARTITIONER_CLASS)), state,
                    builder.getBranches(), builder.getBranch())));
        Preconditions
            .checkArgument(this.builder.get().validatePartitionSchema(this.partitioner.get().partitionSchema()), String
                    .format("Writer %s does not support schema from partitioner %s",
                        builder.getClass().getCanonicalName(), this.partitioner.getClass().getCanonicalName()));
      } catch (ReflectiveOperationException roe) {
        throw new IOException(roe);
      }
    } else {
      this.shouldPartition = false;
      // Support configuration to close the DataWriter on flush to allow publishing intermediate results in a task
      CloseOnFlushWriterWrapper closeOnFlushWriterWrapper =
          new CloseOnFlushWriterWrapper<D>(new Supplier<DataWriter<D>>() {
            @Override
            public DataWriter<D> get() {
              try {
                return builder.withWriterId(PartitionedDataWriter.this.baseWriterId + "_"
                    + PartitionedDataWriter.this.writerIdSuffix++).build();
              } catch (IOException e) {
                throw new RuntimeException("Error creating writer", e);
              }
            }
          }, state);
      DataWriter<D> dataWriter = (DataWriter)closeOnFlushWriterWrapper.getDecoratedObject();

      InstrumentedDataWriterDecorator<D> writer =
          this.closer.register(new InstrumentedDataWriterDecorator<>(closeOnFlushWriterWrapper, state));

      this.isSpeculativeAttemptSafe = this.isDataWriterForPartitionSafe(dataWriter);
      this.isWatermarkCapable = this.isDataWriterWatermarkCapable(dataWriter);
      this.partitionWriters.put(NON_PARTITIONED_WRITER_KEY, writer);
      this.partitioner = Optional.absent();
      this.builder = Optional.absent();
    }
  }

  private boolean isDataWriterWatermarkCapable(DataWriter<D> dataWriter) {
    return (dataWriter instanceof WatermarkAwareWriter) && (((WatermarkAwareWriter) dataWriter).isWatermarkCapable());
  }

  @Override
  public void writeEnvelope(RecordEnvelope<D> recordEnvelope) throws IOException {
    try {
      DataWriter<D> writer = getDataWriterForRecord(recordEnvelope.getRecord());
      writer.writeEnvelope(recordEnvelope);
    } catch (ExecutionException ee) {
      throw new IOException(ee);
    }
  }

  private DataWriter<D> getDataWriterForRecord(D record)
      throws ExecutionException {
    GenericRecord partition =
        this.shouldPartition ? this.partitioner.get().partitionForRecord(record) : NON_PARTITIONED_WRITER_KEY;
    return this.partitionWriters.get(partition);
  }

  @Override
  public void commit()
      throws IOException {
    int writersCommitted = 0;
    for (Map.Entry<GenericRecord, DataWriter<D>> entry : this.partitionWriters.asMap().entrySet()) {
      try {
        entry.getValue().commit();
        writersCommitted++;
      } catch (Throwable throwable) {
        log.error(String.format("Failed to commit writer for partition %s.", entry.getKey()), throwable);
      }
    }
    if (writersCommitted < this.partitionWriters.asMap().size()) {
      throw new IOException("Failed to commit all writers.");
    }
  }

  @Override
  public void cleanup()
      throws IOException {
    int writersCleanedUp = 0;
    for (Map.Entry<GenericRecord, DataWriter<D>> entry : this.partitionWriters.asMap().entrySet()) {
      try {
        entry.getValue().cleanup();
        writersCleanedUp++;
      } catch (Throwable throwable) {
        log.error(String.format("Failed to cleanup writer for partition %s.", entry.getKey()));
      }
    }
    if (writersCleanedUp < this.partitionWriters.asMap().size()) {
      throw new IOException("Failed to clean up all writers.");
    }
  }

  @Override
  public long recordsWritten() {
    long totalRecords = 0;
    for (Map.Entry<GenericRecord, DataWriter<D>> entry : this.partitionWriters.asMap().entrySet()) {
      totalRecords += entry.getValue().recordsWritten();
    }
    return totalRecords;
  }

  @Override
  public long bytesWritten()
      throws IOException {
    long totalBytes = 0;
    for (Map.Entry<GenericRecord, DataWriter<D>> entry : this.partitionWriters.asMap().entrySet()) {
      totalBytes += entry.getValue().bytesWritten();
    }
    return totalBytes;
  }

  @Override
  public void close()
      throws IOException {
    this.closer.close();
  }

  private DataWriter<D> createPartitionWriter(GenericRecord partition)
      throws IOException {
    if (!this.builder.isPresent()) {
      throw new IOException("Writer builder not found. This is an error in the code.");
    }
    DataWriter dataWriter =  this.builder.get().forPartition(partition).withWriterId(this.baseWriterId + "_" + this.writerIdSuffix++)
        .build();
    this.isSpeculativeAttemptSafe = this.isSpeculativeAttemptSafe && this.isDataWriterForPartitionSafe(dataWriter);
    this.isWatermarkCapable = this.isWatermarkCapable && this.isDataWriterWatermarkCapable(dataWriter);
    return dataWriter;
  }

  @Override
  public State getFinalState() {

    State state = new State();
    try {
      for (Map.Entry<GenericRecord, DataWriter<D>> entry : this.partitionWriters.asMap().entrySet()) {
        if (entry.getValue() instanceof FinalState) {

          State partitionFinalState = ((FinalState) entry.getValue()).getFinalState();

          if (this.shouldPartition) {
            for (String key : partitionFinalState.getPropertyNames()) {
              // Prevent overwriting final state across writers
              partitionFinalState.setProp(key + "_" + AvroUtils.serializeAsPath(entry.getKey(), false, true),
                  partitionFinalState.getProp(key));
            }
          }

          state.addAll(partitionFinalState);
        }
      }
      state.setProp("RecordsWritten", recordsWritten());
      state.setProp("BytesWritten", bytesWritten());
    } catch (Exception exception) {
      log.warn("Failed to get final state." + exception.getMessage());
      // If Writer fails to return bytesWritten, it might not be implemented, or implemented incorrectly.
      // Omit property instead of failing.
    }
    return state;
  }

  @Override
  public boolean isSpeculativeAttemptSafe() {
    return this.isSpeculativeAttemptSafe;
  }

  private boolean isDataWriterForPartitionSafe(DataWriter dataWriter) {
    return dataWriter instanceof  SpeculativeAttemptAwareConstruct
        && ((SpeculativeAttemptAwareConstruct) dataWriter).isSpeculativeAttemptSafe();
  }

  @Override
  public boolean isWatermarkCapable() {
    return this.isWatermarkCapable;
  }

  @Override
  public Map<String, CheckpointableWatermark> getCommittableWatermark() {
    // The committable watermark from a collection of commitable and unacknowledged watermarks is the highest
    // committable watermark that is less than the lowest unacknowledged watermark

    WatermarkTracker watermarkTracker = new MultiWriterWatermarkTracker();
    for (Map.Entry<GenericRecord, DataWriter<D>> entry : this.partitionWriters.asMap().entrySet()) {
      if (entry.getValue() instanceof WatermarkAwareWriter) {
        Map<String, CheckpointableWatermark> commitableWatermarks =
            ((WatermarkAwareWriter) entry.getValue()).getCommittableWatermark();
        if (!commitableWatermarks.isEmpty()) {
          watermarkTracker.committedWatermarks(commitableWatermarks);
        }

        Map<String, CheckpointableWatermark> unacknowledgedWatermark =
            ((WatermarkAwareWriter) entry.getValue()).getUnacknowledgedWatermark();
        if (!unacknowledgedWatermark.isEmpty()) {
          watermarkTracker.unacknowledgedWatermarks(unacknowledgedWatermark);
        }
      }
    }
    return watermarkTracker.getAllCommitableWatermarks(); //TODO: Change this to use List of committables instead
  }

  @Override
  public Map<String, CheckpointableWatermark> getUnacknowledgedWatermark() {
    WatermarkTracker watermarkTracker = new MultiWriterWatermarkTracker();
    for (Map.Entry<GenericRecord, DataWriter<D>> entry : this.partitionWriters.asMap().entrySet()) {
      Map<String, CheckpointableWatermark> unacknowledgedWatermark =
          ((WatermarkAwareWriter) entry.getValue()).getUnacknowledgedWatermark();
      if (!unacknowledgedWatermark.isEmpty()) {
        watermarkTracker.unacknowledgedWatermarks(unacknowledgedWatermark);
      }
    }
    return watermarkTracker.getAllUnacknowledgedWatermarks();
  }

  @Override
  public ControlMessageHandler getMessageHandler() {
    return this.controlMessageHandler;
  }

  /**
   * A {@link ControlMessageHandler} that clones the message and lets each writer handle it.
   */
  private class PartitionDataWriterMessageHandler implements ControlMessageHandler {
    @Override
    public void handleMessage(ControlMessage message) {
      StreamEntity.ForkCloner cloner = message.forkCloner();

      // update the schema used to build writers
      if (message instanceof MetadataUpdateControlMessage) {
        PartitionedDataWriter.this.writerBuilder.withSchema(((MetadataUpdateControlMessage) message)
            .getGlobalMetadata().getSchema());
      }

      for (DataWriter writer : PartitionedDataWriter.this.partitionWriters.asMap().values()) {
        ControlMessage cloned = (ControlMessage) cloner.getClone();
        writer.getMessageHandler().handleMessage(cloned);
      }

      cloner.close();
    }
  }
}
