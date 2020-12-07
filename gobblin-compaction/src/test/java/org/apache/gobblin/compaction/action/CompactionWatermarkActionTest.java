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

package org.apache.gobblin.compaction.action;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Optional;

import org.apache.gobblin.compaction.mapreduce.MRCompactor;
import org.apache.gobblin.compaction.verify.CompactionWatermarkChecker;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.data.management.dataset.SimpleFileSystemDataset;
import org.apache.gobblin.dataset.FileSystemDataset;
import org.apache.gobblin.hive.HivePartition;
import org.apache.gobblin.hive.HiveRegister;
import org.apache.gobblin.hive.HiveRegistrationUnit;
import org.apache.gobblin.hive.HiveTable;
import org.apache.gobblin.hive.spec.HiveSpec;


public class CompactionWatermarkActionTest {

  private final String compactionWatermark = "compactionWatermark";
  private final String completionCompactionWatermark = "completionAndCompactionWatermark";

  @Test
  public void testUpdateWatermark()
      throws Exception {
    doTestUpdateWatermark("tracking","event1", "event1");
    doTestUpdateWatermark("db1","table1", "db1/table1");
  }

  private void doTestUpdateWatermark(String db, String table, String dataset)
      throws Exception {
    State state = new State();
    String defaultDb = "tracking";
    state.setProp(CompactionWatermarkAction.DEFAULT_HIVE_DB, defaultDb);

    String inputDir = "/data/tracking";
    String inputSubDir = "hourly";
    String destSubDir = "daily";
    String datasetPath = String.format("%s/%s/%s/2019/12/20", inputDir, dataset, inputSubDir);
    state.setProp(MRCompactor.COMPACTION_INPUT_DIR, inputDir);
    state.setProp(MRCompactor.COMPACTION_DEST_DIR, inputDir);
    state.setProp(MRCompactor.COMPACTION_INPUT_SUBDIR, inputSubDir);
    state.setProp(MRCompactor.COMPACTION_DEST_SUBDIR, destSubDir);
    state.setProp(HiveRegister.HIVE_REGISTER_TYPE, MockHiveRegister.class.getName());
    state.setProp(CompactionWatermarkAction.GRANULARITY, "DAY");

    State tableProps = new State();
    // 2019-12-31 23:59:59.999
    String existingWatermark = "1577836799999";
    tableProps.setProp(compactionWatermark, existingWatermark);
    tableProps.setProp(completionCompactionWatermark, existingWatermark);
    HiveTable existingTable = new HiveTable.Builder().withDbName(db).withTableName(table)
        .withProps(tableProps).build();
    MockHiveRegister.existingTable = existingTable;

    CompactionWatermarkAction action = new CompactionWatermarkAction(state);
    FileSystemDataset fsDataset = new SimpleFileSystemDataset(new Path(datasetPath));

    // Will not update if old watermarks are reported
    String actualWatermark = "1577750399999"; // 2019-10-30 23:59:59.999
    doWatermarkTest(action, fsDataset, state, actualWatermark, existingWatermark);

    // Will throw a runtime exception if watermark is not continuous
    Exception exception = null;
    try {
      actualWatermark = "1578009599999"; // 2020-01-01 23:59:59.999
      doWatermarkTest(action, fsDataset, state, actualWatermark, actualWatermark);
    } catch (Exception e) {
      exception = e;
    }
    Assert.assertEquals(exception.getMessage(),
        String.format("Fail to advance %s of dataset %s: expect 1577923199999 but got %s, "
            + "please manually fill the gap and rerun.",
            compactionWatermark, fsDataset.datasetRoot(), actualWatermark));

    // Will update if newer continuous watermarks are reported
    actualWatermark = "1577923199999"; // 2020-01-01 23:59:59.999
    doWatermarkTest(action, fsDataset, state, actualWatermark, actualWatermark);
  }

  @Test
  public void testWatermarkWithDST() throws Exception {
    // Test case 1
    // Time zone: PST(America/Los_Angeles)
    // Existing watermark millis: 1583654399999 (2020-03-07 23:59:59.999 PST)
    // Actual watermark millis: 1583737199999 (2020-03-08 23:59:59.999 PST) with DST
    testWatermarkWithDSTTimeZone("America/Los_Angeles", "1583654399999", "1583737199999");
    // Test case 2
    // Time zone: UTC
    // Existing watermark millis: 1583625599999 (2020-03-07 23:59:59.999 UTC)
    // Actual watermark millis: 1583711999999 (2020-03-08 23:59:59.999 UTC)
    testWatermarkWithDSTTimeZone("UTC", "1583625599999", "1583711999999");
  }

  private void testWatermarkWithDSTTimeZone(String timeZone, String existingWatermark, String actualWatermark)
      throws Exception {
    String db = "db1";
    String table = "table1";
    String dataset = "db1/table1";
    State state = new State();
    String defaultDb = "tracking";
    state.setProp(CompactionWatermarkAction.DEFAULT_HIVE_DB, defaultDb);

    String inputDir = "/data/tracking";
    String inputSubDir = "hourly";
    String destSubDir = "daily";
    String datasetPath = String.format("%s/%s/%s/2020/03/08", inputDir, dataset, inputSubDir);
    state.setProp(MRCompactor.COMPACTION_INPUT_DIR, inputDir);
    state.setProp(MRCompactor.COMPACTION_DEST_DIR, inputDir);
    state.setProp(MRCompactor.COMPACTION_INPUT_SUBDIR, inputSubDir);
    state.setProp(MRCompactor.COMPACTION_DEST_SUBDIR, destSubDir);
    state.setProp(HiveRegister.HIVE_REGISTER_TYPE, MockHiveRegister.class.getName());
    state.setProp(CompactionWatermarkAction.GRANULARITY, "DAY");
    state.setProp(MRCompactor.COMPACTION_TIMEZONE, timeZone);

    State tableProps = new State();
    tableProps.setProp(compactionWatermark, existingWatermark);
    tableProps.setProp(completionCompactionWatermark, existingWatermark);
    HiveTable existingTable = new HiveTable.Builder().withDbName(db).withTableName(table)
        .withProps(tableProps).build();
    MockHiveRegister.existingTable = existingTable;

    CompactionWatermarkAction action = new CompactionWatermarkAction(state);
    FileSystemDataset fsDataset = new SimpleFileSystemDataset(new Path(datasetPath));

    doWatermarkTest(action, fsDataset, state, actualWatermark, actualWatermark);
  }

  private void doWatermarkTest(CompactionWatermarkAction action, FileSystemDataset fsDataset,
      State state, String actualWatermark, String expectedWatermark)
      throws Exception {
    state.setProp(CompactionWatermarkChecker.COMPACTION_WATERMARK, actualWatermark);
    state.setProp(CompactionWatermarkChecker.COMPLETION_COMPACTION_WATERMARK, actualWatermark);

    action.onCompactionJobComplete(fsDataset);

    Assert.assertEquals(MockHiveRegister.existingTable.getProps().getProp(compactionWatermark),
        expectedWatermark);
    Assert.assertEquals(MockHiveRegister.existingTable.getProps().getProp(completionCompactionWatermark),
        expectedWatermark);
  }

  public static class MockHiveRegister extends HiveRegister {

    static HiveTable existingTable;

    public MockHiveRegister(State state, Optional<String> uri) {
      super(state);
    }

    @Override
    protected void registerPath(HiveSpec spec)
        throws IOException {

    }

    @Override
    public boolean createDbIfNotExists(String dbName)
        throws IOException {
      return false;
    }

    @Override
    public boolean createTableIfNotExists(HiveTable table)
        throws IOException {
      return false;
    }

    @Override
    public boolean addPartitionIfNotExists(HiveTable table, HivePartition partition)
        throws IOException {
      return false;
    }

    @Override
    public boolean existsTable(String dbName, String tableName)
        throws IOException {
      return false;
    }

    @Override
    public boolean existsPartition(String dbName, String tableName, List<HiveRegistrationUnit.Column> partitionKeys,
        List<String> partitionValues)
        throws IOException {
      return false;
    }

    @Override
    public void dropTableIfExists(String dbName, String tableName)
        throws IOException {

    }

    @Override
    public void dropPartitionIfExists(String dbName, String tableName, List<HiveRegistrationUnit.Column> partitionKeys,
        List<String> partitionValues)
        throws IOException {

    }

    @Override
    public Optional<HiveTable> getTable(String dbName, String tableName)
        throws IOException {
      if (dbName.equals(existingTable.getDbName())
          && tableName.equals(existingTable.getTableName())) {
        return Optional.of(existingTable);
      }
      return Optional.absent();
    }

    @Override
    public Optional<HivePartition> getPartition(String dbName, String tableName,
        List<HiveRegistrationUnit.Column> partitionKeys, List<String> partitionValues)
        throws IOException {
      return null;
    }

    @Override
    public void alterTable(HiveTable table)
        throws IOException {
      existingTable = table;
    }

    @Override
    public void alterPartition(HiveTable table, HivePartition partition)
        throws IOException {

    }
  }
}
