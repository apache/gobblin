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
package org.apache.gobblin.util.test;

import java.io.IOException;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.source.extractor.DataRecordException;
import org.apache.gobblin.source.extractor.Extractor;
import org.apache.gobblin.source.workunit.WorkUnit;

/**
 * Unit tests for {@link StressTestingSource}
 */
public class TestStressTestingSource {

  @Test
  public void testSourceExtractor() throws DataRecordException, IOException {
    final int MEM_ALLOC_BYTES = 100;
    final int NUM_WORK_UNITS = 10;
    final int COMPUTE_TIME_MICRO = 10;
    final int NUM_RECORDS = 10000;


    SourceState state = new SourceState();
    state.setProp(StressTestingSource.NUM_WORK_UNITS_KEY, NUM_WORK_UNITS);
    state.setProp(StressTestingSource.MEM_ALLOC_BYTES_KEY, MEM_ALLOC_BYTES);
    state.setProp(StressTestingSource.COMPUTE_TIME_MICRO_KEY, COMPUTE_TIME_MICRO);
    state.setProp(StressTestingSource.NUM_RECORDS_KEY, NUM_RECORDS);

    StressTestingSource source = new StressTestingSource();

    List<WorkUnit> wus = source.getWorkunits(state);
    Assert.assertEquals(wus.size(), NUM_WORK_UNITS);

    for (int i = 0; i < wus.size(); ++i) {
      WorkUnit wu = wus.get(i);
      WorkUnitState wuState = new WorkUnitState(wu, state);
      Extractor<String, byte[]> extractor = source.getExtractor(wuState);

      Assert.assertEquals(extractor.getExpectedRecordCount(), NUM_RECORDS);
      Assert.assertEquals(extractor.readRecord(null).length, 100);
    }
  }

  @Test
  public void testComputeTime() throws DataRecordException, IOException {
    final int MEM_ALLOC_BYTES = 100;
    final int NUM_WORK_UNITS = 1;
    final int COMPUTE_TIME_MICRO = 10000;
    final int NUM_RECORDS = 500;

    SourceState state = new SourceState();
    state.setProp(StressTestingSource.NUM_WORK_UNITS_KEY, NUM_WORK_UNITS);
    state.setProp(StressTestingSource.MEM_ALLOC_BYTES_KEY, MEM_ALLOC_BYTES);
    state.setProp(StressTestingSource.COMPUTE_TIME_MICRO_KEY, COMPUTE_TIME_MICRO);
    state.setProp(StressTestingSource.NUM_RECORDS_KEY, NUM_RECORDS);

    StressTestingSource source = new StressTestingSource();

    List<WorkUnit> wus = source.getWorkunits(state);
    Assert.assertEquals(wus.size(), NUM_WORK_UNITS);

    WorkUnit wu = wus.get(0);
    WorkUnitState wuState = new WorkUnitState(wu, state);
    Extractor<String, byte[]> extractor = source.getExtractor(wuState);

    byte[] record;
    long startTimeNano = System.nanoTime();
    while ((record = extractor.readRecord(null)) != null) {
      Assert.assertEquals(record.length, 100);
    }
    long endTimeNano = System.nanoTime();

    long timeSpentMicro = (endTimeNano - startTimeNano)/(1000);
    // check that there is less than 5 second difference between expected and actual time spent
    Assert.assertTrue(Math.abs(timeSpentMicro - (COMPUTE_TIME_MICRO * NUM_RECORDS)) < (5000000),
        "Time spent " + timeSpentMicro);
  }

  @Test
  public void testSleepTime() throws DataRecordException, IOException {
    final int MEM_ALLOC_BYTES = 100;
    final int NUM_WORK_UNITS = 1;
    final int SLEEP_TIME_MICRO = 10000;
    final int NUM_RECORDS = 500;

    SourceState state = new SourceState();
    state.setProp(StressTestingSource.NUM_WORK_UNITS_KEY, NUM_WORK_UNITS);
    state.setProp(StressTestingSource.MEM_ALLOC_BYTES_KEY, MEM_ALLOC_BYTES);
    state.setProp(StressTestingSource.SLEEP_TIME_MICRO_KEY, SLEEP_TIME_MICRO);
    state.setProp(StressTestingSource.NUM_RECORDS_KEY, NUM_RECORDS);

    StressTestingSource source = new StressTestingSource();

    List<WorkUnit> wus = source.getWorkunits(state);
    Assert.assertEquals(wus.size(), NUM_WORK_UNITS);

    WorkUnit wu = wus.get(0);
    WorkUnitState wuState = new WorkUnitState(wu, state);
    Extractor<String, byte[]> extractor = source.getExtractor(wuState);

    byte[] record;
    long startTimeNano = System.nanoTime();
    while ((record = extractor.readRecord(null)) != null) {
      Assert.assertEquals(record.length, 100);
    }
    long endTimeNano = System.nanoTime();

    long timeSpentMicro = (endTimeNano - startTimeNano)/(1000);
    // check that there is less than 2 second difference between expected and actual time spent
    Assert.assertTrue(Math.abs(timeSpentMicro - (SLEEP_TIME_MICRO * NUM_RECORDS)) < (2000000),
        "Time spent " + timeSpentMicro);
  }

  @Test
  public void testRunDuration() throws DataRecordException, IOException {
    final int MEM_ALLOC_BYTES = 100;
    final int NUM_WORK_UNITS = 1;
    final int SLEEP_TIME_MICRO = 1000;
    final int NUM_RECORDS = 30; // this config is ignored since the duration is set
    final int RUN_DURATION_SECS = 5;

    SourceState state = new SourceState();
    state.setProp(StressTestingSource.NUM_WORK_UNITS_KEY, NUM_WORK_UNITS);
    state.setProp(StressTestingSource.MEM_ALLOC_BYTES_KEY, MEM_ALLOC_BYTES);
    state.setProp(StressTestingSource.SLEEP_TIME_MICRO_KEY, SLEEP_TIME_MICRO);
    state.setProp(StressTestingSource.NUM_RECORDS_KEY, NUM_RECORDS);
    state.setProp(StressTestingSource.RUN_DURATION_KEY, RUN_DURATION_SECS);

    StressTestingSource source = new StressTestingSource();

    List<WorkUnit> wus = source.getWorkunits(state);
    Assert.assertEquals(wus.size(), NUM_WORK_UNITS);

    WorkUnit wu = wus.get(0);
    WorkUnitState wuState = new WorkUnitState(wu, state);
    Extractor<String, byte[]> extractor = source.getExtractor(wuState);

    byte[] record;
    long startTimeNano = System.nanoTime();
    while ((record = extractor.readRecord(null)) != null) {
      Assert.assertEquals(record.length, 100);
    }
    long endTimeNano = System.nanoTime();

    long timeSpentMicro = (endTimeNano - startTimeNano)/(1000);
    // check that there is less than 1 second difference between expected and actual time spent
    Assert.assertTrue(Math.abs(timeSpentMicro - (RUN_DURATION_SECS * 1000000)) < (1000000),
        "Time spent " + timeSpentMicro);
  }
}
