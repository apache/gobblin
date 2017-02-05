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

package gobblin.test;

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

import lombok.extern.slf4j.Slf4j;

import gobblin.configuration.SourceState;
import gobblin.configuration.WorkUnitState;
import gobblin.source.Source;
import gobblin.source.extractor.CheckpointableWatermark;
import gobblin.source.extractor.DataRecordException;
import gobblin.source.extractor.DefaultCheckpointableWatermark;
import gobblin.source.extractor.RecordEnvelope;
import gobblin.source.extractor.StreamingExtractor;
import gobblin.source.extractor.WatermarkInterval;
import gobblin.source.extractor.extract.LongWatermark;
import gobblin.source.workunit.Extract;
import gobblin.source.workunit.ExtractFactory;
import gobblin.source.workunit.WorkUnit;
import gobblin.util.ConfigUtils;
import gobblin.writer.WatermarkStorage;


/**
 * A Test source that generates a continuous stream of records
 */
@Slf4j
public class SequentialStreamingTestSource implements Source<String, RecordEnvelope<String>> {
  private static final int DEFAULT_NUM_PARALLELISM = 1;
  private static final String DEFAULT_NAMESPACE = "TestDB";
  private static final String DEFAULT_TABLE = "TestTable";
  private static final Long DEFAULT_SLEEP_TIME_PER_RECORD_MILLIS = 10L;
  public static final String WORK_UNIT_INDEX = "workUnitIndex";

  private final AtomicBoolean configured = new AtomicBoolean(false);
  private int num_parallelism;
  private String namespace;
  private String table;
  private long sleepTimePerRecord;
  private final Extract.TableType tableType = Extract.TableType.APPEND_ONLY;
  private final ExtractFactory _extractFactory = new ExtractFactory("yyyyMMddHHmmss");

  private void configureIfNeeded(Config config)
  {
    if (!configured.get()) {
      num_parallelism = ConfigUtils.getInt(config, "source.numParallelism", DEFAULT_NUM_PARALLELISM);
      namespace = ConfigUtils.getString(config, "source.namespace", DEFAULT_NAMESPACE);
      table = ConfigUtils.getString(config, "source.table", DEFAULT_TABLE);
      sleepTimePerRecord = ConfigUtils.getLong(config, "source.sleepTimePerRecordMillis",
          DEFAULT_SLEEP_TIME_PER_RECORD_MILLIS);
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
          LongWatermark expectedWatermark = new LongWatermark(Long.MAX_VALUE);
          WatermarkInterval watermarkInterval = new WatermarkInterval(watermark, expectedWatermark);
          workUnit = WorkUnit.create(newExtract(tableType, namespace, table), watermarkInterval);
          log.debug("Will be setting watermark interval to " + watermarkInterval.toJson());
          workUnit.setProp(WORK_UNIT_INDEX, workUnitState.getWorkunit().getProp(WORK_UNIT_INDEX));
        }
        else
        {
          // retry
          LongWatermark watermark = workUnitState.getWorkunit().getLowWatermark(LongWatermark.class);
          LongWatermark expectedWatermark = new LongWatermark(Long.MAX_VALUE);
          WatermarkInterval watermarkInterval = new WatermarkInterval(watermark, expectedWatermark);
          workUnit = WorkUnit.create(newExtract(tableType, namespace, table), watermarkInterval);
          log.debug("Will be setting watermark interval to " + watermarkInterval.toJson());
          workUnit.setProp(WORK_UNIT_INDEX, workUnitState.getWorkunit().getProp(WORK_UNIT_INDEX));
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
      LongWatermark lowWatermark = new LongWatermark(1);
      LongWatermark expectedHighWatermark = new LongWatermark(Long.MAX_VALUE);
      workUnit.setWatermarkInterval(new WatermarkInterval(lowWatermark, expectedHighWatermark));
      workUnit.setProp(WORK_UNIT_INDEX, i);
      workUnits.add(workUnit);
    }
    return workUnits;
  }

  private Extract newExtract(Extract.TableType tableType, String namespace, String table) {
    return _extractFactory.getUniqueExtract(tableType, namespace, table);
  }



  private class SequentialStreamingExtractor implements StreamingExtractor<String, String> {
    private int index;
    private LongWatermark currentWatermark;
    private final LongWatermark lowWatermark;
    private long recordsExtracted;
    private final WorkUnitState workUnitState;
    private Optional<WatermarkStorage> watermarkStorage;


    SequentialStreamingExtractor(WorkUnitState state) {
      this.lowWatermark = state.getWorkunit().getLowWatermark(LongWatermark.class);
      this.workUnitState = state;
      this.index = state.getPropAsInt(WORK_UNIT_INDEX);
      this.recordsExtracted = 0;
      this.currentWatermark = lowWatermark;
      this.watermarkStorage = Optional.absent();
      log.info("Constructed instance {} ", this);
    }

    @Override
    public String getSchema()
        throws IOException {
      return "";
    }

    @Override
    public RecordEnvelope<String> readRecord(@Deprecated RecordEnvelope<String> reuse)
        throws DataRecordException, IOException {
      try {
        Thread.sleep(sleepTimePerRecord);
        this.currentWatermark.increment();
        String record = ":index:" + index + ":seq:" + this.currentWatermark.getValue() + ":";
        log.info("{}: Extracted record -> {}", this, record);
        return new RecordEnvelope<>(record, new DefaultCheckpointableWatermark("" + index, new LongWatermark(currentWatermark.getValue())));
      } catch (InterruptedException e) {
        throw new IOException("Interrupted while reading record", e);
      } finally {
        recordsExtracted++;
      }
    }

    @Override
    public long getExpectedRecordCount() {
      return recordsExtracted;
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

    @Override
    public void start(WatermarkStorage watermarkStorage) throws IOException {
      this.watermarkStorage = Optional.of(watermarkStorage);
      Map<String, CheckpointableWatermark> lastCommitted;
      try {
        lastCommitted = this.watermarkStorage.get()
            .getCommittedWatermarks(DefaultCheckpointableWatermark.class, ImmutableList.of("" + index));
      } catch (IOException e) {
        // failed to get watermarks ... log a warning message
        log.warn("Failed to get watermarks... will start from the beginning", e);
        lastCommitted = Collections.EMPTY_MAP;
      }
      for (Map.Entry entry: lastCommitted.entrySet()) {
        log.info("{}: Found these committed watermarks: key: {}, value: {}", this, entry.getKey(), entry.getValue());
      }
      if (!lastCommitted.isEmpty() && lastCommitted.containsKey(""+index)) {
        this.currentWatermark = (LongWatermark) (lastCommitted.get("" + index)).getWatermark();
      } else {
        // first record starts from 0
        this.currentWatermark = new LongWatermark(-1);
      }
      log.info("{}: Set current watermark to : {}", this, this.currentWatermark);
    }
  }

  @Override
  public StreamingExtractor<String, String> getExtractor(WorkUnitState state)
      throws IOException {
    configureIfNeeded(ConfigFactory.parseProperties(state.getProperties()));
    return new SequentialStreamingExtractor(state);
  }

  @Override
  public void shutdown(SourceState state) {

  }
}
