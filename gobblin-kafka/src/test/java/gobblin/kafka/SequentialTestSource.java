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

package gobblin.kafka;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import lombok.extern.slf4j.Slf4j;

import gobblin.configuration.SourceState;
import gobblin.configuration.WorkUnitState;
import gobblin.source.Source;
import gobblin.source.extractor.DataRecordException;
import gobblin.source.extractor.Extractor;
import gobblin.source.extractor.WatermarkInterval;
import gobblin.source.extractor.extract.LongWatermark;
import gobblin.source.workunit.Extract;
import gobblin.source.workunit.ExtractFactory;
import gobblin.source.workunit.WorkUnit;
import gobblin.util.ConfigUtils;


/**
 * A Test source that generates a sequence of records
 */
@Slf4j
public class SequentialTestSource implements Source<String, String> {
  private static final int DEFAULT_NUM_PARALLELISM = 1;
  private static final String DEFAULT_NAMESPACE = "TestDB";
  private static final String DEFAULT_TABLE = "TestTable";
  private static final Integer DEFAULT_NUM_RECORDS_PER_EXTRACT = 100;
  public static final String WORK_UNIT_INDEX = "workUnitIndex";

  private final AtomicBoolean configured = new AtomicBoolean(false);
  private int num_parallelism;
  private String namespace;
  private String table;
  private int numRecordsPerExtract;
  private final Extract.TableType tableType = Extract.TableType.APPEND_ONLY;
  private final ExtractFactory _extractFactory = new ExtractFactory("yyyyMMddHHmmss");

  private void configureIfNeeded(Config config)
  {
    if (!configured.get()) {
      num_parallelism = ConfigUtils.getInt(config, "source.numParallelism", DEFAULT_NUM_PARALLELISM);
      namespace = ConfigUtils.getString(config, "source.namespace", DEFAULT_NAMESPACE);
      table = ConfigUtils.getString(config, "source.table", DEFAULT_TABLE);
      numRecordsPerExtract = ConfigUtils.getInt(config, "source.numRecordsPerExtract", DEFAULT_NUM_RECORDS_PER_EXTRACT);
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
      workUnits.add(workUnit);
    }
    return workUnits;
  }

  private Extract newExtract(Extract.TableType tableType, String namespace, String table) {
    return _extractFactory.getUniqueExtract(tableType, namespace, table);
  }

  @Override
  public Extractor<String, String> getExtractor(WorkUnitState state)
      throws IOException {
    configureIfNeeded(ConfigFactory.parseProperties(state.getProperties()));
    final LongWatermark lowWatermark = state.getWorkunit().getLowWatermark(LongWatermark.class);
    final WorkUnitState workUnitState = state;
    final int index = state.getPropAsInt(WORK_UNIT_INDEX);
    return new Extractor<String, String>() {
      long recordsExtracted = 0;
      LongWatermark currentWatermark = lowWatermark;
      @Override
      public String getSchema()
          throws IOException {
        return "";
      }

      @Override
      public String readRecord(@Deprecated String reuse)
          throws DataRecordException, IOException {
        if (recordsExtracted < numRecordsPerExtract) {
          String record = ":index:" + index + ":seq:" + currentWatermark.getValue() + ":";
          log.debug("Extracted record -> " + record);
          currentWatermark.increment();
          recordsExtracted++;
          return record;
        }
        else
        {
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
    };
  }

  @Override
  public void shutdown(SourceState state) {

  }
}
