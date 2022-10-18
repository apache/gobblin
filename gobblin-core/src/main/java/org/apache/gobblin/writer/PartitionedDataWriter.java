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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.reflect.ConstructorUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.commit.SpeculativeAttemptAwareConstruct;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.dataset.Descriptor;
import org.apache.gobblin.dataset.PartitionDescriptor;
import org.apache.gobblin.exception.NonTransientException;
import org.apache.gobblin.instrumented.writer.InstrumentedDataWriterDecorator;
import org.apache.gobblin.instrumented.writer.InstrumentedPartitionedDataWriterDecorator;
import org.apache.gobblin.records.ControlMessageHandler;
import org.apache.gobblin.stream.ControlMessage;
import org.apache.gobblin.stream.FlushControlMessage;
import org.apache.gobblin.stream.MetadataUpdateControlMessage;
import org.apache.gobblin.stream.RecordEnvelope;
import org.apache.gobblin.stream.StreamEntity;
import org.apache.gobblin.util.AvroUtils;
import org.apache.gobblin.util.ExecutorsUtils;
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

  public static final String WRITER_LATEST_SCHEMA = "writer.latest.schema";
  //Config to control when a writer is evicted from Partitioned data writer cache.
  // NOTE: this config must be set only in streaming mode. For batch mode, setting this config will result
  // in incorrect behavior.
  public static final String PARTITIONED_WRITER_CACHE_TTL_SECONDS = "partitionedDataWriter.cache.ttl.seconds";
  public static final Long DEFAULT_PARTITIONED_WRITER_CACHE_TTL_SECONDS = Long.MAX_VALUE;
  public static final String PARTITIONED_WRITER_WRITE_TIMEOUT_SECONDS = "partitionedDataWriter.write.timeout.seconds";
  public static final Long DEFAULT_PARTITIONED_WRITER_WRITE_TIMEOUT_SECONDS = Long.MAX_VALUE;

  private static final GenericRecord NON_PARTITIONED_WRITER_KEY =
      new GenericData.Record(SchemaBuilder.record("Dummy").fields().endRecord());

  private int writerIdSuffix = 0;
  private final String baseWriterId;
  private final State state;
  private final int branchId;

  private final Optional<WriterPartitioner> partitioner;
  @Getter
  @VisibleForTesting
  private final LoadingCache<GenericRecord, DataWriter<D>> partitionWriters;
  private final Optional<PartitionAwareDataWriterBuilder> builder;
  private final DataWriterBuilder writerBuilder;
  private final boolean shouldPartition;
  private final Closer closer;
  private final ControlMessageHandler controlMessageHandler;
  private boolean isSpeculativeAttemptSafe;
  private boolean isWatermarkCapable;
  private long writeTimeoutInterval;

  private ScheduledExecutorService cacheCleanUpExecutor;

  //Counters to keep track of records and bytes of writers which have been evicted from cache.
  @Getter
  @VisibleForTesting
  private long totalRecordsFromEvictedWriters;
  @Getter
  @VisibleForTesting
  private long totalBytesFromEvictedWriters;


  public PartitionedDataWriter(DataWriterBuilder<S, D> builder, final State state)
      throws IOException {
    this.state = state;
    this.branchId = builder.branch;

    this.isSpeculativeAttemptSafe = true;
    this.isWatermarkCapable = true;
    this.baseWriterId = builder.getWriterId();
    this.closer = Closer.create();
    this.writerBuilder = builder;
    this.controlMessageHandler = new PartitionDataWriterMessageHandler();
    if(builder.schema != null) {
      this.state.setProp(WRITER_LATEST_SCHEMA, builder.getSchema());
    }
    long cacheExpiryInterval = this.state.getPropAsLong(PARTITIONED_WRITER_CACHE_TTL_SECONDS, DEFAULT_PARTITIONED_WRITER_CACHE_TTL_SECONDS);
    this.writeTimeoutInterval = this.state.getPropAsLong(PARTITIONED_WRITER_WRITE_TIMEOUT_SECONDS,
        DEFAULT_PARTITIONED_WRITER_WRITE_TIMEOUT_SECONDS);
    // Bound the timeout value to avoid data loss when slow write happening
    this.writeTimeoutInterval = Math.min(this.writeTimeoutInterval, cacheExpiryInterval / 3 * 2);
    log.debug("PartitionedDataWriter: Setting cache expiry interval to {} seconds", cacheExpiryInterval);

    this.partitionWriters = CacheBuilder.newBuilder()
        .expireAfterAccess(cacheExpiryInterval, TimeUnit.SECONDS)
        .removalListener(new RemovalListener<GenericRecord, DataWriter<D>>() {
      @Override
      public void onRemoval(RemovalNotification<GenericRecord, DataWriter<D>> notification) {
        synchronized (PartitionedDataWriter.this) {
          if (notification.getValue() != null) {
            try {
              DataWriter<D> writer = notification.getValue();
              totalRecordsFromEvictedWriters += writer.recordsWritten();
              totalBytesFromEvictedWriters += writer.bytesWritten();
              writer.close();
            } catch (IOException e) {
              log.error("Exception {} encountered when closing data writer on cache eviction", e);
              //Should propagate the exception to avoid committing/publishing corrupt files.
              throw new RuntimeException(e);
            }
          }
        }
      }
    }).build(new CacheLoader<GenericRecord, DataWriter<D>>() {
      @Override
      public DataWriter<D> load(final GenericRecord key)
          throws Exception {
        /* wrap the data writer to allow the option to close the writer on flush */
        return new InstrumentedPartitionedDataWriterDecorator<>(
                new CloseOnFlushWriterWrapper<D>(new Supplier<DataWriter<D>>() {
                  @Override
                  public DataWriter<D> get() {
                    try {
                      log.info(String.format("Adding one more writer to loading cache of existing writer "
                          + "with size = %d", partitionWriters.size()));
                      return createPartitionWriter(key);
                    } catch (IOException e) {
                      throw new RuntimeException("Error creating writer", e);
                    }
                  }
                }, state), state, key);
      }
    });

    //Schedule a DataWriter cache clean up operation, since LoadingCache may keep the object
    // in memory even after it has been evicted from the cache.
    if (cacheExpiryInterval < Long.MAX_VALUE) {
      this.cacheCleanUpExecutor = Executors.newSingleThreadScheduledExecutor(
          ExecutorsUtils.newThreadFactory(Optional.of(log), Optional.of("CacheCleanupExecutor")));
      this.cacheCleanUpExecutor.scheduleAtFixedRate(new Runnable() {
        @Override
        public void run() {
          PartitionedDataWriter.this.partitionWriters.cleanUp();
        }
      }, 0, cacheExpiryInterval, TimeUnit.SECONDS);
    }

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
      GenericRecord partition = getPartitionForRecord(recordEnvelope.getRecord());
      DataWriter<D> writer = this.partitionWriters.get(partition);
      long startTime = System.currentTimeMillis();
      writer.writeEnvelope(recordEnvelope);
      long timeForWriting = System.currentTimeMillis() - startTime;
      // If the write take a long time, which is 1/3 of cache expiration time, we fail the writer to avoid data loss
      // and further slowness on the same HDFS block
      if (timeForWriting / 1000 > this.writeTimeoutInterval ) {
        //Use NonTransientException to avoid writer retry, in this case, retry will also cause data loss
        throw new NonTransientException(String.format("Write record took %s s, but threshold is %s s",
            timeForWriting / 1000, writeTimeoutInterval));
      }
    } catch (ExecutionException ee) {
      throw new IOException(ee);
    }
  }

  private GenericRecord getPartitionForRecord(D record) {
     return this.shouldPartition ? this.partitioner.get().partitionForRecord(record) : NON_PARTITIONED_WRITER_KEY;
  }

  @Override
  public synchronized void commit()
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
  public synchronized void cleanup()
      throws IOException {
    int writersCleanedUp = 0;
    for (Map.Entry<GenericRecord, DataWriter<D>> entry : this.partitionWriters.asMap().entrySet()) {
      try {
        entry.getValue().cleanup();
        writersCleanedUp++;
      } catch (Throwable throwable) {
        log.error(String.format("Failed to cleanup writer for partition %s.", entry.getKey()), throwable);
      }
    }
    if (writersCleanedUp < this.partitionWriters.asMap().size()) {
      throw new IOException("Failed to clean up all writers.");
    }
  }

  @Override
  public synchronized long recordsWritten() {
    long totalRecords = 0;
    for (Map.Entry<GenericRecord, DataWriter<D>> entry : this.partitionWriters.asMap().entrySet()) {
      totalRecords += entry.getValue().recordsWritten();
    }
    return totalRecords + this.totalRecordsFromEvictedWriters;
  }

  @Override
  public synchronized long bytesWritten()
      throws IOException {
    long totalBytes = 0;
    for (Map.Entry<GenericRecord, DataWriter<D>> entry : this.partitionWriters.asMap().entrySet()) {
      totalBytes += entry.getValue().bytesWritten();
    }
    return totalBytes + this.totalBytesFromEvictedWriters;
  }

  @Override
  public synchronized void close()
      throws IOException {
    try {
      serializePartitionInfoToState();
    } finally {
      closeWritersInCache();
      this.closer.close();
    }
  }

  private void closeWritersInCache() throws IOException {
    for (Map.Entry<GenericRecord, DataWriter<D>> entry : this.partitionWriters.asMap().entrySet()) {
      entry.getValue().close();
    }
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
  public synchronized State getFinalState() {

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
      log.warn("Failed to get final state.", exception);
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
        state.setProp(WRITER_LATEST_SCHEMA, ((MetadataUpdateControlMessage) message)
            .getGlobalMetadata().getSchema());
      } else if (message instanceof FlushControlMessage){
        //Add Partition info to state to report partition level lineage events on Flush
        serializePartitionInfoToState();
      }

      synchronized (PartitionedDataWriter.this) {
        for (DataWriter writer : PartitionedDataWriter.this.partitionWriters.asMap().values()) {
          ControlMessage cloned = (ControlMessage) cloner.getClone();
          writer.getMessageHandler().handleMessage(cloned);
        }
      }

      cloner.close();
    }
  }

  /**
   * Get the serialized key to partitions info in {@link #state}
   */
  public static String getPartitionsKey(int branchId) {
    return String.format("writer.%d.partitions", branchId);
  }

  /**
   * Serialize partitions info to {@link #state} if they are any
   */
  private void serializePartitionInfoToState() {
    List<PartitionDescriptor> descriptors = new ArrayList<>();

    for (DataWriter writer : partitionWriters.asMap().values()) {
      Descriptor descriptor = writer.getDataDescriptor();
      if (null == descriptor) {
        log.warn("Drop partition info as writer {} returns a null PartitionDescriptor", writer.toString());
        continue;
      }

      if (!(descriptor instanceof PartitionDescriptor)) {
        log.warn("Drop partition info as writer {} does not return a PartitionDescriptor", writer.toString());
        continue;
      }

      descriptors.add((PartitionDescriptor)descriptor);
    }

    if (descriptors.size() > 0) {
      state.setProp(getPartitionsKey(branchId), PartitionDescriptor.toPartitionJsonList(descriptors));
    } else {
      log.info("Partitions info not available. Will not serialize partitions");
    }
  }

  /**
   * Get the partition info of a work unit from the {@code state}. Then partition info will be removed from the
   * {@code state} to avoid persisting useless information
   *
   * <p>
   *   In Gobblin, only the {@link PartitionedDataWriter} knows all partitions written for a work unit. Each partition
   *   {@link DataWriter} decides the actual form of a dataset partition
   * </p>
   */
  public static List<PartitionDescriptor> getPartitionInfoAndClean(State state, int branchId) {
    String partitionsKey = getPartitionsKey(branchId);
    String json = state.getProp(partitionsKey);
    if (Strings.isNullOrEmpty(json)) {
      return Lists.newArrayList();
    }
    state.removeProp(partitionsKey);
    return PartitionDescriptor.fromPartitionJsonList(json);
  }
}
