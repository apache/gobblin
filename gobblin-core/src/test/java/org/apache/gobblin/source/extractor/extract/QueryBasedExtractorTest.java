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
package org.apache.gobblin.source.extractor.extract;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.source.extractor.DataRecordException;
import org.apache.gobblin.source.extractor.exception.HighWatermarkException;
import org.apache.gobblin.source.extractor.exception.RecordCountException;
import org.apache.gobblin.source.extractor.exception.SchemaException;
import org.apache.gobblin.source.extractor.partition.Partition;
import org.apache.gobblin.source.extractor.partition.Partitioner;
import org.apache.gobblin.source.extractor.watermark.Predicate;
import org.apache.gobblin.source.extractor.watermark.WatermarkPredicate;
import org.apache.gobblin.source.extractor.watermark.WatermarkType;
import org.apache.gobblin.source.workunit.WorkUnit;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Unit tests for {@link QueryBasedExtractor}
 */
public class QueryBasedExtractorTest {
  @Test
  public void testDataPullUpperBoundsRemovedInLastWorkUnit() {
    int totalCount = 5;
    ArrayList<DataRecord> records = this.generateRecords(totalCount);

    WorkUnit workUnit = WorkUnit.createEmpty();
    workUnit.setProp(Partition.IS_LAST_PARTIITON, true);
    workUnit.setProp(ConfigurationKeys.SOURCE_QUERYBASED_EXTRACT_TYPE, "SNAPSHOT");
    WorkUnitState workUnitState = new WorkUnitState(workUnit, new State());
    workUnitState.setId("testDataPullUpperBoundsRemovedInLastWorkUnit");

    TestQueryBasedExtractor testExtractor = new TestQueryBasedExtractor(workUnitState, records);
    testExtractor.setRangePredicates(1, 3);
    this.verify(testExtractor, totalCount);
  }

  @Test
  public void testDataPullUpperBoundsNotRemovedInLastWorkUnit() {
    int totalCount = 5;
    ArrayList<DataRecord> records = this.generateRecords(totalCount);

    WorkUnit workUnit = WorkUnit.createEmpty();
    WorkUnitState workUnitState = new WorkUnitState(workUnit, new State());
    workUnitState.setId("testDataPullUpperBoundsNotRemovedInLastWorkUnit");

    // It's not a last work unit
    TestQueryBasedExtractor testExtractor = new TestQueryBasedExtractor(workUnitState, records);
    testExtractor.setRangePredicates(1, 3);
    this.verify(testExtractor, 3);

    // It's a last work unit but user specifies high watermark
    workUnit.setProp(Partition.IS_LAST_PARTIITON, true);
    workUnit.setProp(Partition.HAS_USER_SPECIFIED_HIGH_WATERMARK, true);
    testExtractor.reset();
    testExtractor.setRangePredicates(1, 3);
    this.verify(testExtractor, 3);

    // It's a last work unit but it has WORK_UNIT_STATE_ACTUAL_HIGH_WATER_MARK_KEY on record
    workUnit.removeProp(Partition.HAS_USER_SPECIFIED_HIGH_WATERMARK);
    workUnit.setProp(ConfigurationKeys.WORK_UNIT_STATE_ACTUAL_HIGH_WATER_MARK_KEY, "3");
    testExtractor.reset();
    testExtractor.setRangePredicates(1, 3);
    this.verify(testExtractor, 3);
  }

  private ArrayList<DataRecord> generateRecords(int count) {
    ArrayList<DataRecord> records = new ArrayList<>();
    while (count > 0) {
      records.add(new DataRecord(count, count));
      count--;
    }
    return records;
  }

  private void verify(TestQueryBasedExtractor testExtractor, int expectedCount) {
    int actualCount = 0;
    try {
      while (testExtractor.readRecord(null) != null) {
        actualCount++;
      }
    } catch (Exception e) {
      Assert.fail("There should not incur any exception");
    }
    Assert.assertEquals(actualCount, expectedCount, "Expect " + expectedCount + " records!");
  }

  private class TestQueryBasedExtractor extends QueryBasedExtractor<ArrayList, DataRecord> {
    private final ArrayList<DataRecord> records;
    private long previousActualHwmValue;

    TestQueryBasedExtractor(WorkUnitState workUnitState, ArrayList<DataRecord> records) {
      super(workUnitState);
      this.records = records;
      previousActualHwmValue = -1;
    }

    void setRangePredicates(long lwmValue, long hwmValue) {
      WatermarkPredicate watermark = new WatermarkPredicate("timeStamp", WatermarkType.SIMPLE);
      predicateList.add(watermark.getPredicate(this, lwmValue, ">=", Predicate.PredicateType.LWM));
      predicateList.add(watermark.getPredicate(this, hwmValue, "<=", Predicate.PredicateType.HWM));
    }

    void reset() {
      previousActualHwmValue = -1;
      predicateList.clear();
      setFetchStatus(true);
    }

    @Override
    public void extractMetadata(String schema, String entity, WorkUnit workUnit) throws SchemaException, IOException {

    }

    @Override
    public long getMaxWatermark(String schema, String entity, String watermarkColumn,
        List<Predicate> snapshotPredicateList, String watermarkSourceFormat) throws HighWatermarkException {
      return 0;
    }

    @Override
    public long getSourceCount(String schema, String entity, WorkUnit workUnit, List<Predicate> predicateList)
        throws RecordCountException {
      return records.size();
    }

    @Override
    public Iterator<DataRecord> getRecordSet(String schema, String entity, WorkUnit workUnit, List<Predicate> predicateList)
        throws DataRecordException, IOException {
      if (records == null || predicateList == null) {
        // No new data to pull
        return null;
      }

      long lwmValue = -1;
      long hwmValue = Long.MAX_VALUE;
      long actualHwmValue = -1;
      // Adjust watermarks from predicate list
      for (Predicate predicate: predicateList) {
        if (predicate.getType() == Predicate.PredicateType.HWM) {
          hwmValue = predicate.value;
        }
        if (predicate.getType() == Predicate.PredicateType.LWM) {
          lwmValue = predicate.value;
        }
      }

      ArrayList<DataRecord> filteredRecords = new ArrayList<>();
      for (DataRecord record : records) {
        if (record.timeStamp <= previousActualHwmValue) {
          // The record has been pulled previously
          continue;
        }
        if (record.timeStamp >= lwmValue && record.timeStamp <= hwmValue) {
          // Make a copy
          filteredRecords.add(new DataRecord(record.value, record.timeStamp));
          // Mark actual high watermark
          if (record.timeStamp > actualHwmValue) {
            actualHwmValue = record.timeStamp;
          }
        }
      }

      if (filteredRecords.isEmpty()) {
        return null;
      }
      previousActualHwmValue = actualHwmValue;
      return filteredRecords.iterator();
    }

    @Override
    public String getWatermarkSourceFormat(WatermarkType watermarkType) {
      return null;
    }

    @Override
    public String getHourPredicateCondition(String column, long value, String valueFormat, String operator) {
      return null;
    }

    @Override
    public String getDatePredicateCondition(String column, long value, String valueFormat, String operator) {
      return null;
    }

    @Override
    public String getTimestampPredicateCondition(String column, long value, String valueFormat, String operator) {
      return null;
    }

    @Override
    public void setTimeOut(int timeOut) {

    }

    @Override
    public Map<String, String> getDataTypeMap() {
      return null;
    }

    @Override
    public void closeConnection() throws Exception {

    }

    @Override
    public Iterator<DataRecord> getRecordSetFromSourceApi(String schema, String entity, WorkUnit workUnit,
        List<Predicate> predicateList) throws IOException {
      try {
        return getRecordSet(schema, entity, workUnit, predicateList);
      } catch (DataRecordException e) {
        e.printStackTrace();
        return null;
      }
    }
  }

  private class DataRecord {
    int value;
    long timeStamp;

    DataRecord(int value, long timeStamp) {
      this.value = value;
      this.timeStamp = timeStamp;
    }
  }
}
