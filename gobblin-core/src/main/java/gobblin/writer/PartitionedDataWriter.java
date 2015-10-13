/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
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

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.instrumented.writer.InstrumentedDataWriterDecorator;
import gobblin.instrumented.writer.InstrumentedPartitionedDataWriterDecorator;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang.reflect.ConstructorUtils;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

/**
 * {@link DataWriter} that partitions data using a partitioner, instantiates appropriate writers, and sends records to
 * the chosen writer.
 */
public class PartitionedDataWriter<S, D> implements DataWriter<D> {

  private static final GenericRecord NON_PARTITIONED_WRITER_KEY =
      new GenericData.Record(SchemaBuilder.record("Dummy").fields().endRecord());

  private final Optional<WriterPartitioner> partitioner;
  private final LoadingCache<GenericRecord, DataWriter<D>> partitionWriters;
  private final Optional<PartitionAwareDataWriterBuilder> builder;
  private final boolean shouldPartition;

  public PartitionedDataWriter(DataWriterBuilder<S, D> builder, final State state) throws IOException {

    this.partitionWriters = CacheBuilder.newBuilder().build(new CacheLoader<GenericRecord, DataWriter<D>>() {
      @Override public DataWriter<D> load(final GenericRecord key) throws Exception {
        return new InstrumentedPartitionedDataWriterDecorator<D>(createPartitionWriter(key), state, key);
      }
    });

    if(state.contains(ConfigurationKeys.WRITER_PARTITIONER_CLASS)) {
      Preconditions.checkArgument(builder instanceof PartitionAwareDataWriterBuilder,
          String.format("%s was specified but the writer %s does not support partitioning.",
              ConfigurationKeys.WRITER_PARTITIONER_CLASS, builder.getClass().getCanonicalName()));

      try {
        this.shouldPartition = true;
        this.builder = Optional.of(PartitionAwareDataWriterBuilder.class.cast(builder));
        this.partitioner = Optional.of(
            WriterPartitioner.class.cast(ConstructorUtils.invokeConstructor(
                    Class.forName(state.getProp(ConfigurationKeys.WRITER_PARTITIONER_CLASS)), state)));
        this.builder.get().validatePartitionSchema(this.partitioner.get().partitionSchema());
      } catch(ReflectiveOperationException roe) {
        throw new IOException(roe);
      }
    } else {
      this.shouldPartition = false;
      this.partitionWriters.put(NON_PARTITIONED_WRITER_KEY,
          new InstrumentedDataWriterDecorator<D>(builder.build(), state));
      this.partitioner = Optional.absent();
      this.builder = Optional.absent();
    }
  }

  @Override public void write(D record) throws IOException {
    try {
      GenericRecord partition = this.shouldPartition ? this.partitioner.get().partitionForRecord(record) :
          NON_PARTITIONED_WRITER_KEY;
      DataWriter<D> writer = this.partitionWriters.get(partition);
      writer.write(record);
    } catch(ExecutionException ee) {
      throw new IOException(ee);
    }
  }

  @Override public void commit() throws IOException {
    for(Map.Entry<GenericRecord, DataWriter<D>> entry : this.partitionWriters.asMap().entrySet()) {
      entry.getValue().commit();
    }
  }

  @Override public void cleanup() throws IOException {
    for(Map.Entry<GenericRecord, DataWriter<D>> entry : this.partitionWriters.asMap().entrySet()) {
      entry.getValue().cleanup();
    }
  }

  @Override public long recordsWritten() {
    long totalRecords = 0;
    for(Map.Entry<GenericRecord, DataWriter<D>> entry : this.partitionWriters.asMap().entrySet()) {
      totalRecords += entry.getValue().recordsWritten();
    }
    return totalRecords;
  }

  @Override public long bytesWritten() throws IOException {
    long totalBytes = 0;
    for(Map.Entry<GenericRecord, DataWriter<D>> entry : this.partitionWriters.asMap().entrySet()) {
      totalBytes += entry.getValue().bytesWritten();
    }
    return totalBytes;
  }

  @Override public void close() throws IOException {
    for(Map.Entry<GenericRecord, DataWriter<D>> entry : this.partitionWriters.asMap().entrySet()) {
      entry.getValue().close();
    }
  }

  private DataWriter<D> createPartitionWriter(GenericRecord partition) throws IOException {
    if(!this.builder.isPresent()) {
      throw new IOException("Writer builder not found. This is an error in the code.");
    }
    return this.builder.get().forPartition(partition).build();
  }
}
