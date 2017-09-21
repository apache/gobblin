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
package org.apache.gobblin.data.management.conversion.hive;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Table;
import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.data.management.ConversionHiveTestUtils;
import org.apache.gobblin.data.management.conversion.hive.source.HiveSource;
import org.apache.gobblin.data.management.conversion.hive.source.HiveWorkUnit;
import org.apache.gobblin.data.management.conversion.hive.watermarker.PartitionLevelWatermarker;
import org.apache.gobblin.data.management.conversion.hive.watermarker.TableLevelWatermarker;
import org.apache.gobblin.source.workunit.WorkUnit;


@Test(groups = { "gobblin.data.management.conversion" })
public class HiveSourceTest {

  private LocalHiveMetastoreTestUtils hiveMetastoreTestUtils;
  private HiveSource hiveSource;

  @BeforeClass
  public void setup() throws Exception {
    this.hiveMetastoreTestUtils = LocalHiveMetastoreTestUtils.getInstance();
    this.hiveSource = new HiveSource();
  }

  @Test
  public void testGetWorkUnitsForTable() throws Exception {

    String dbName = "testdb2";
    String tableName = "testtable2";
    String tableSdLoc = "/tmp/testtable2";

    this.hiveMetastoreTestUtils.getLocalMetastoreClient().dropDatabase(dbName, false, true, true);

    SourceState testState = getTestState(dbName);

    this.hiveMetastoreTestUtils.createTestAvroTable(dbName, tableName, tableSdLoc, Optional.<String> absent());

    List<WorkUnit> workUnits = hiveSource.getWorkunits(testState);

    // One workunit for the table, no dummy workunits
    Assert.assertEquals(workUnits.size(), 1);
    WorkUnit wu = workUnits.get(0);

    HiveWorkUnit hwu = new HiveWorkUnit(wu);

    Assert.assertEquals(hwu.getHiveDataset().getDbAndTable().getDb(), dbName);
    Assert.assertEquals(hwu.getHiveDataset().getDbAndTable().getTable(), tableName);
    Assert.assertEquals(hwu.getTableSchemaUrl(), new Path("/tmp/dummy"));
  }

  @Test
  public void testGetWorkUnitsForPartitions() throws Exception {

    String dbName = "testdb3";
    String tableName = "testtable3";
    String tableSdLoc = "/tmp/testtable3";

    this.hiveMetastoreTestUtils.getLocalMetastoreClient().dropDatabase(dbName, false, true, true);

    SourceState testState = getTestState(dbName);

    Table tbl = this.hiveMetastoreTestUtils.createTestAvroTable(dbName, tableName, tableSdLoc, Optional.of("field"));

    this.hiveMetastoreTestUtils.addTestPartition(tbl, ImmutableList.of("f1"), (int) System.currentTimeMillis());

    List<WorkUnit> workUnits = this.hiveSource.getWorkunits(testState);

    // One workunit for the partition + 1 dummy watermark workunit
    Assert.assertEquals(workUnits.size(), 2);
    WorkUnit wu = workUnits.get(0);
    WorkUnit wu2 = workUnits.get(1);

    HiveWorkUnit hwu = null;
    if (!wu.contains(PartitionLevelWatermarker.IS_WATERMARK_WORKUNIT_KEY)) {
      hwu = new HiveWorkUnit(wu);
    } else {
      hwu = new HiveWorkUnit(wu2);
    }

    Assert.assertEquals(hwu.getHiveDataset().getDbAndTable().getDb(), dbName);
    Assert.assertEquals(hwu.getHiveDataset().getDbAndTable().getTable(), tableName);
    Assert.assertEquals(hwu.getPartitionName().get(), "field=f1");
  }

  @Test
  public void testGetWorkunitsAfterWatermark() throws Exception {

    String dbName = "testdb4";
    String tableName1 = "testtable1";
    String tableSdLoc1 = "/tmp/testtable1";
    String tableName2 = "testtable2";
    String tableSdLoc2 = "/tmp/testtable2";

    this.hiveMetastoreTestUtils.getLocalMetastoreClient().dropDatabase(dbName, false, true, true);

    this.hiveMetastoreTestUtils.createTestAvroTable(dbName, tableName1, tableSdLoc1, Optional.<String> absent());
    this.hiveMetastoreTestUtils.createTestAvroTable(dbName, tableName2, tableSdLoc2, Optional.<String> absent(), true);

    List<WorkUnitState> previousWorkUnitStates = Lists.newArrayList();

    Table table1 = this.hiveMetastoreTestUtils.getLocalMetastoreClient().getTable(dbName, tableName1);

    previousWorkUnitStates.add(ConversionHiveTestUtils.createWus(dbName, tableName1,
        TimeUnit.MILLISECONDS.convert(table1.getCreateTime(), TimeUnit.SECONDS)));

    SourceState testState = new SourceState(getTestState(dbName), previousWorkUnitStates);
    testState.setProp(HiveSource.HIVE_SOURCE_WATERMARKER_FACTORY_CLASS_KEY, TableLevelWatermarker.Factory.class.getName());

    List<WorkUnit> workUnits = this.hiveSource.getWorkunits(testState);

    Assert.assertEquals(workUnits.size(), 1);
    WorkUnit wu = workUnits.get(0);

    HiveWorkUnit hwu = new HiveWorkUnit(wu);

    Assert.assertEquals(hwu.getHiveDataset().getDbAndTable().getDb(), dbName);
    Assert.assertEquals(hwu.getHiveDataset().getDbAndTable().getTable(), tableName2);
  }

  @Test
  public void testShouldCreateWorkunitsOlderThanLookback() throws Exception {

    long currentTime = System.currentTimeMillis();
    long partitionCreateTime = new DateTime(currentTime).minusDays(35).getMillis();

    org.apache.hadoop.hive.ql.metadata.Partition partition =
        this.hiveMetastoreTestUtils.createDummyPartition(partitionCreateTime);

    SourceState testState = getTestState("testDb6");
    HiveSource source = new HiveSource();
    source.initialize(testState);

    boolean isOlderThanLookback = source.isOlderThanLookback(partition);

    Assert.assertEquals(isOlderThanLookback, true, "Should not create workunits older than lookback");
  }

  @Test
  public void testShouldCreateWorkunitsNewerThanLookback() throws Exception {

    long currentTime = System.currentTimeMillis();
    // Default lookback time is 3 days
    long partitionCreateTime = new DateTime(currentTime).minusDays(2).getMillis();

    org.apache.hadoop.hive.ql.metadata.Partition partition =
        this.hiveMetastoreTestUtils.createDummyPartition(partitionCreateTime);

    SourceState testState = getTestState("testDb7");
    HiveSource source = new HiveSource();
    source.initialize(testState);

    boolean isOlderThanLookback = source.isOlderThanLookback(partition);

    Assert.assertEquals(isOlderThanLookback, false, "Should create workunits newer than lookback");
  }

  @Test
  public void testIsOlderThanLookbackForDistcpGenerationTime() throws Exception {

    long currentTime = System.currentTimeMillis();
    // Default lookback time is 3 days
    long partitionCreateTime = new DateTime(currentTime).minusDays(2).getMillis();
    Map<String, String> parameters = Maps.newHashMap();
    parameters.put(HiveSource.DISTCP_REGISTRATION_GENERATION_TIME_KEY, partitionCreateTime + "");

    org.apache.hadoop.hive.ql.metadata.Partition partition = this.hiveMetastoreTestUtils.createDummyPartition(0);
    partition.getTPartition().setParameters(parameters);

    SourceState testState = getTestState("testDb6");
    HiveSource source = new HiveSource();
    source.initialize(testState);

    boolean isOlderThanLookback = source.isOlderThanLookback(partition);

    Assert.assertEquals(isOlderThanLookback, false, "Should create workunits newer than lookback");
  }

  private static SourceState getTestState(String dbName) {
    SourceState testState = new SourceState();
    testState.setProp("hive.dataset.database", dbName);
    testState.setProp("hive.dataset.table.pattern", "*");
    testState.setProp(ConfigurationKeys.JOB_ID_KEY, "testJobId");

    return testState;
  }

}
