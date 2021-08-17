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

package org.apache.gobblin.test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import com.google.common.base.Throwables;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.source.Source;
import org.apache.gobblin.source.extractor.CheckpointableWatermark;
import org.apache.gobblin.source.extractor.DataRecordException;
import org.apache.gobblin.source.extractor.DefaultCheckpointableWatermark;
import org.apache.gobblin.source.extractor.Extractor;
import org.apache.gobblin.source.extractor.StreamingExtractor;
import org.apache.gobblin.source.extractor.WatermarkInterval;
import org.apache.gobblin.source.extractor.extract.LongWatermark;
import org.apache.gobblin.source.workunit.Extract;
import org.apache.gobblin.source.workunit.ExtractFactory;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.stream.RecordEnvelope;
import org.apache.gobblin.test.proto.TestRecordProtos;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.writer.WatermarkStorage;

/**
 * A Test source that generates a sequence of records, works in batch and streaming mode.
 */
@Slf4j
public class SequentialTestSource implements Source<Object, Object> {

  private enum InMemoryFormat {
    POJO,
    AVRO,
    PROTOBUF
  }


  private static final int DEFAULT_NUM_PARALLELISM = 1;
  private static final String DEFAULT_NAMESPACE = "TestDB";
  private static final String DEFAULT_TABLE = "TestTable";
  private static final Integer DEFAULT_NUM_RECORDS_PER_EXTRACT = 100;
  public static final String WORK_UNIT_INDEX = "workUnitIndex";
  private static final Long DEFAULT_SLEEP_TIME_PER_RECORD_MILLIS = 10L;
  public static final String MEMORY_FORMAT_KEY = "inMemoryFormat";
  public static final String DEFAULT_IN_MEMORY_FORMAT = InMemoryFormat.POJO.toString();


  private final AtomicBoolean configured = new AtomicBoolean(false);
  private int num_parallelism;
  private String namespace;
  private String table;
  private int numRecordsPerExtract;
  private long sleepTimePerRecord;
  private InMemoryFormat inMemFormat;
  private final Extract.TableType tableType = Extract.TableType.APPEND_ONLY;
  private final ExtractFactory _extractFactory = new ExtractFactory("yyyyMMddHHmmss");
  private boolean streaming = false;

  private void configureIfNeeded(Config config)
  {
    if (!configured.get()) {
      num_parallelism = ConfigUtils.getInt(config, "source.numParallelism", DEFAULT_NUM_PARALLELISM);
      namespace = ConfigUtils.getString(config, "source.namespace", DEFAULT_NAMESPACE);
      table = ConfigUtils.getString(config, "source.table", DEFAULT_TABLE);
      numRecordsPerExtract = ConfigUtils.getInt(config, "source.numRecordsPerExtract", DEFAULT_NUM_RECORDS_PER_EXTRACT);
      sleepTimePerRecord = ConfigUtils.getLong(config, "source.sleepTimePerRecordMillis",
          DEFAULT_SLEEP_TIME_PER_RECORD_MILLIS);
      streaming = (ConfigUtils.getString(config, "task.executionMode", "BATCH").equalsIgnoreCase("STREAMING"));
      if (streaming) {
        numRecordsPerExtract = Integer.MAX_VALUE;
      }
      inMemFormat = InMemoryFormat.valueOf(ConfigUtils.getString(config, "source." + MEMORY_FORMAT_KEY,
          DEFAULT_IN_MEMORY_FORMAT));
      log.info("Source configured with: num_parallelism: {}, namespace: {}, "
          + "table: {}, numRecordsPerExtract: {}, sleepTimePerRecord: {}, streaming: {}, inMemFormat: {}",
          this.num_parallelism, this.namespace,
          this.table, this.numRecordsPerExtract, this.sleepTimePerRecord, this.streaming, this.inMemFormat);
      configured.set(true);
    }
  }


  @Override
  public List<WorkUnit> getWorkunits(SourceState state) {
    configureIfNeeded(ConfigFactory.parseProperties(state.getProperties()));
    final List<WorkUnitState> previousWorkUnitStates = state.getPreviousWorkUnitStates();
    if (!previousWorkUnitStates.isEmpty())
    {
      List<WorkUnit> newWorkUnits = Lists.newArrayListWithCapacity(previousWorkUnitStates.size());
      int i = 0;
      for (WorkUnitState workUnitState: previousWorkUnitStates)
      {
        WorkUnit workUnit;
        if (workUnitState.getWorkingState().equals(WorkUnitState.WorkingState.COMMITTED))
        {
          LongWatermark watermark = workUnitState.getActualHighWatermark(LongWatermark.class);
          LongWatermark expectedWatermark = new LongWatermark(watermark.getValue() + numRecordsPerExtract);
          WatermarkInterval watermarkInterval = new WatermarkInterval(watermark, expectedWatermark);
          workUnit = WorkUnit.create(newExtract(tableType, namespace, table), watermarkInterval);
          log.debug("Will be setting watermark interval to " + watermarkInterval.toJson());
          workUnit.setProp(WORK_UNIT_INDEX, workUnitState.getWorkunit().getProp(WORK_UNIT_INDEX));
          workUnit.setProp(MEMORY_FORMAT_KEY, this.inMemFormat.toString());
        }
        else
        {
          // retry
          LongWatermark watermark = workUnitState.getWorkunit().getLowWatermark(LongWatermark.class);
          LongWatermark expectedWatermark = new LongWatermark(watermark.getValue() + numRecordsPerExtract);
          WatermarkInterval watermarkInterval = new WatermarkInterval(watermark, expectedWatermark);
          workUnit = WorkUnit.create(newExtract(tableType, namespace, table), watermarkInterval);
          log.debug("Will be setting watermark interval to " + watermarkInterval.toJson());
          workUnit.setProp(WORK_UNIT_INDEX, workUnitState.getWorkunit().getProp(WORK_UNIT_INDEX));
          workUnit.setProp(MEMORY_FORMAT_KEY, this.inMemFormat.toString());
        }
        newWorkUnits.add(workUnit);
      }
      return newWorkUnits;
    }
    else {
      return initialWorkUnits();
    }
  }

  private List<WorkUnit> initialWorkUnits() {
    List<WorkUnit> workUnits = Lists.newArrayList();
    for (int i=0; i < num_parallelism; i++)
    {
      WorkUnit workUnit = WorkUnit.create(newExtract(Extract.TableType.APPEND_ONLY, namespace,
          table));
      LongWatermark lowWatermark = new LongWatermark(i * numRecordsPerExtract + 1);
      LongWatermark expectedHighWatermark = new LongWatermark((i + 1) * numRecordsPerExtract);
      workUnit.setWatermarkInterval(new WatermarkInterval(lowWatermark, expectedHighWatermark));
      workUnit.setProp(WORK_UNIT_INDEX, i);
      workUnit.setProp(MEMORY_FORMAT_KEY, this.inMemFormat.toString());
      workUnits.add(workUnit);
    }
    return workUnits;
  }

  private Extract newExtract(Extract.TableType tableType, String namespace, String table) {
    return _extractFactory.getUniqueExtract(tableType, namespace, table);
  }


  static class TestBatchExtractor implements Extractor<Object, Object> {
    private long recordsExtracted = 0;
    private final long numRecordsPerExtract;
    private LongWatermark currentWatermark;
    private long sleepTimePerRecord;
    private int partition;
    private final InMemoryFormat inMemoryFormat;
    private final Object schema;
    WorkUnitState workUnitState;


    public TestBatchExtractor(int partition,
        LongWatermark lowWatermark,
        long numRecordsPerExtract,
        long sleepTimePerRecord,
        WorkUnitState wuState) {
      this.partition = partition;
      this.currentWatermark = lowWatermark;
      this.numRecordsPerExtract = numRecordsPerExtract;
      this.sleepTimePerRecord = sleepTimePerRecord;
      this.workUnitState = wuState;
      this.inMemoryFormat = InMemoryFormat.valueOf(this.workUnitState.getProp(MEMORY_FORMAT_KEY));
      this.schema = getSchema(inMemoryFormat);
    }

      @Override
      public Object getSchema()
          throws IOException {
        return this.schema;
      }

      private Object getSchema(InMemoryFormat inMemoryFormat) {
        switch (inMemoryFormat) {
          case POJO: {
            return TestRecord.class;
          }
          case AVRO: {
            return org.apache.gobblin.test.avro.TestRecord.getClassSchema();
          }
          case PROTOBUF: {
            return TestRecordProtos.TestRecord.class;
          }
          default:
            throw new RuntimeException("Not implemented " + inMemoryFormat.name());
        }
      }

      @Override
      public RecordEnvelope readRecordEnvelope()
          throws DataRecordException, IOException {
        if (recordsExtracted < numRecordsPerExtract) {
          try {
            Thread.sleep(sleepTimePerRecord);
          } catch (InterruptedException e) {
            Throwables.propagate(e);
          }
          Object record;
          switch (this.inMemoryFormat) {
            case POJO: {
              record = new TestRecord(this.partition, this.currentWatermark.getValue(), "I am a POJO message");
              break;
            }
            case AVRO: {
              record = org.apache.gobblin.test.avro.TestRecord.newBuilder()
                  .setPartition(this.partition)
                  .setSequence(this.currentWatermark.getValue())
                  .setPayload("I am an Avro message")
                  .build();
              break;
            }
            case PROTOBUF: {
              record = TestRecordProtos.TestRecord.newBuilder()
                  .setPartition(this.partition)
                  .setSequence(this.currentWatermark.getValue())
                  .setPayload("I am a Protobuf message")
                  .build();
              break;
            }
            default: throw new RuntimeException("");
          }
          log.debug("Extracted record -> {}", record);
          RecordEnvelope re = new RecordEnvelope<>(record,
              new DefaultCheckpointableWatermark(String.valueOf(this.partition),
              new LongWatermark(this.currentWatermark.getValue())));
          currentWatermark.increment();
          recordsExtracted++;
          return re;
        } else {
          return null;
        }
      }

      @Override
      public long getExpectedRecordCount() {
        return numRecordsPerExtract;
      }

      @Override
      public long getHighWatermark() {
        return workUnitState.getHighWaterMark();
      }

      @Override
      public void close()
          throws IOException {
        workUnitState.setActualHighWatermark(currentWatermark);
      }

    public void setCurrentWatermark(LongWatermark currentWatermark) {
      this.currentWatermark = currentWatermark;
    }
  }


  static class TestStreamingExtractor implements StreamingExtractor<Object, Object> {
    private Optional<WatermarkStorage> watermarkStorage;
    private final TestBatchExtractor extractor;

    public TestStreamingExtractor(TestBatchExtractor extractor) {
      this.extractor = extractor;
    }

    @Override
    public void close()
    throws IOException {
      extractor.close();
    }

    @Override
    public Object getSchema()
    throws IOException {
      return extractor.getSchema();
    }

    @Override
    public RecordEnvelope<Object> readRecordEnvelope()
    throws DataRecordException, IOException {
      return extractor.readRecordEnvelope();
    }

    @Override
    public long getExpectedRecordCount() {
      return extractor.getExpectedRecordCount();
    }

    @Override
    public long getHighWatermark() {
      return extractor.getHighWatermark();
    }

    @Override
    public void start(WatermarkStorage watermarkStorage)
    throws IOException {
      this.watermarkStorage = Optional.of(watermarkStorage);
      Map<String, CheckpointableWatermark> lastCommitted;
      try {
        lastCommitted = this.watermarkStorage.get()
            .getCommittedWatermarks(DefaultCheckpointableWatermark.class, ImmutableList.of("" + extractor.partition));
      } catch (IOException e) {
        // failed to get watermarks ... log a warning message
        log.warn("Failed to get watermarks... will start from the beginning", e);
        lastCommitted = Collections.EMPTY_MAP;
      }
      for (Map.Entry entry: lastCommitted.entrySet()) {
        log.info("{}: Found these committed watermarks: key: {}, value: {}", this, entry.getKey(), entry.getValue());
      }
      LongWatermark currentWatermark;
      if (!lastCommitted.isEmpty() && lastCommitted.containsKey(""+extractor.partition)) {
        currentWatermark = (LongWatermark) (lastCommitted.get("" + extractor.partition)).getWatermark();
      } else {
        // first record starts from 0
        currentWatermark = new LongWatermark(-1);
      }
      extractor.setCurrentWatermark(currentWatermark);
      log.info("{}: Set current watermark to : {}", this, currentWatermark);

    }
  };



  @Override
  public Extractor<Object, Object> getExtractor(WorkUnitState state)
      throws IOException {
    Config config = ConfigFactory.parseProperties(state.getProperties());
    configureIfNeeded(config);
    final LongWatermark lowWatermark = state.getWorkunit().getLowWatermark(LongWatermark.class);
    final WorkUnitState workUnitState = state;
    final int index = state.getPropAsInt(WORK_UNIT_INDEX);
    final TestBatchExtractor extractor = new TestBatchExtractor(index, lowWatermark, numRecordsPerExtract,
        sleepTimePerRecord, workUnitState);
    if (!streaming) {
      return extractor;
    } else {
      return (Extractor) new TestStreamingExtractor(extractor);
  }
  }

  @Override
  public void shutdown(SourceState state) {

  }
}
