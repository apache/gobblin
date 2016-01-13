/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.writer;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;

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

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.instrumented.writer.InstrumentedDataWriterDecorator;
import gobblin.instrumented.writer.InstrumentedPartitionedDataWriterDecorator;
import gobblin.util.AvroUtils;
import gobblin.util.FinalState;
import gobblin.writer.partitioner.WriterPartitioner;


/**
 * {@link DataWriter} that partitions data using a partitioner, instantiates appropriate writers, and sends records to
 * the chosen writer.
 * @param <S> schema type.
 * @param <D> record type.
 */
@Slf4j
public class PartitionedDataWriter<S, D> implements DataWriter<D>, FinalState {

  private static final GenericRecord NON_PARTITIONED_WRITER_KEY =
      new GenericData.Record(SchemaBuilder.record("Dummy").fields().endRecord());

  private int writerIdSuffix = 0;
  private final String baseWriterId;
  private final Optional<WriterPartitioner> partitioner;
  private final LoadingCache<GenericRecord, DataWriter<D>> partitionWriters;
  private final Optional<PartitionAwareDataWriterBuilder> builder;
  private final boolean shouldPartition;
  private final Closer closer;

  public PartitionedDataWriter(DataWriterBuilder<S, D> builder, final State state) throws IOException {

    this.baseWriterId = builder.getWriterId();
    this.closer = Closer.create();
    this.partitionWriters = CacheBuilder.newBuilder().build(new CacheLoader<GenericRecord, DataWriter<D>>() {
      @Override
      public DataWriter<D> load(final GenericRecord key) throws Exception {
        return closer
            .register(new InstrumentedPartitionedDataWriterDecorator<D>(createPartitionWriter(key), state, key));
      }
    });

    if (state.contains(ConfigurationKeys.WRITER_PARTITIONER_CLASS)) {
      Preconditions.checkArgument(builder instanceof PartitionAwareDataWriterBuilder,
          String.format("%s was specified but the writer %s does not support partitioning.",
              ConfigurationKeys.WRITER_PARTITIONER_CLASS, builder.getClass().getCanonicalName()));

      try {
        this.shouldPartition = true;
        this.builder = Optional.of(PartitionAwareDataWriterBuilder.class.cast(builder));
        this.partitioner = Optional.of(WriterPartitioner.class.cast(
            ConstructorUtils.invokeConstructor(Class.forName(state.getProp(ConfigurationKeys.WRITER_PARTITIONER_CLASS)),
                state, builder.getBranches(), builder.getBranch())));
        Preconditions.checkArgument(
            this.builder.get().validatePartitionSchema(this.partitioner.get().partitionSchema()),
            String.format("Writer %s does not support schema from partitioner %s",
                builder.getClass().getCanonicalName(), this.partitioner.getClass().getCanonicalName()));
      } catch (ReflectiveOperationException roe) {
        throw new IOException(roe);
      }
    } else {
      this.shouldPartition = false;
      InstrumentedDataWriterDecorator<D> writer =
          this.closer.register(new InstrumentedDataWriterDecorator<D>(builder.build(), state));
      this.partitionWriters.put(NON_PARTITIONED_WRITER_KEY, writer);
      this.partitioner = Optional.absent();
      this.builder = Optional.absent();
    }
  }

  @Override
  public void write(D record) throws IOException {
    try {
      GenericRecord partition =
          this.shouldPartition ? this.partitioner.get().partitionForRecord(record) : NON_PARTITIONED_WRITER_KEY;
      DataWriter<D> writer = this.partitionWriters.get(partition);
      writer.write(record);
    } catch (ExecutionException ee) {
      throw new IOException(ee);
    }
  }

  @Override
  public void commit() throws IOException {
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
  public void cleanup() throws IOException {
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
  public long bytesWritten() throws IOException {
    long totalBytes = 0;
    for (Map.Entry<GenericRecord, DataWriter<D>> entry : this.partitionWriters.asMap().entrySet()) {
      totalBytes += entry.getValue().bytesWritten();
    }
    return totalBytes;
  }

  @Override
  public void close() throws IOException {
    this.closer.close();
  }

  private DataWriter<D> createPartitionWriter(GenericRecord partition) throws IOException {
    if (!this.builder.isPresent()) {
      throw new IOException("Writer builder not found. This is an error in the code.");
    }
    return this.builder.get().forPartition(partition).withWriterId(this.baseWriterId + "_" + this.writerIdSuffix++)
        .build();
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
}
