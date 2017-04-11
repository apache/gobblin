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
package gobblin.data.management.conversion.hive.watermarker;

import com.google.common.base.Optional;
import com.google.common.io.Files;
import gobblin.data.management.conversion.hive.LocalHiveMetastoreTestUtils;
import java.io.File;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.tools.ant.taskdefs.Local;
import org.joda.time.DateTime;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.SourceState;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.data.management.conversion.hive.converter.AbstractAvroToOrcConverter;
import gobblin.data.management.conversion.hive.source.HiveSource;
import gobblin.source.extractor.extract.LongWatermark;
import gobblin.source.workunit.WorkUnit;


/*
 * Tests covered
 *
 * // Previous state tests
 * 1. Test previousWatermarks reading.
 * 1.5 Null previous watermarks in a watermark workunit
 * 2. Test more than one watermark workunits
 * 3 Get previous high watermark for partition
 *
 *  // Callback tests
 *  4. Test calling onPartitionProcessBegin before OnTableProcessBegin
 *  5. Test remove dropped partitions
 *  6. Test addPartitionWatermarks
 *
 *  // OnGetWorkunitsEnd tests
 *  7.  No previous state. 5 most recently modified partitions
 *  8. Previous state 3. New partitions 3. 2 from new state retained
 *  9. Previous state 4. New partitions 5. All 5 new retained
 *  10. Previous state 5. New partitions 3; 2 existing and 1 new
 *  11. Previous state 3, 2 dropped. New partitions 2
 */
@Test(groups = { "gobblin.data.management.conversion" })
public class PartitionLevelWatermarkerTest {

  @Test
  public void testExpectedHighWatermarkNoPreviousState() throws Exception {

    String dbName = "testExpectedHighWatermarkNoPreviousState";
    LocalHiveMetastoreTestUtils.getInstance().dropDatabaseIfExists(dbName);

    long now = new DateTime().getMillis();

    SourceState state = new SourceState();
    PartitionLevelWatermarker watermarker = new PartitionLevelWatermarker(state);

    Table table = localTestTable(dbName, "testTable1", true);
    Partition part1 = localTestPartition(table, Lists.newArrayList("2015"));

    watermarker.onTableProcessBegin(table, 0l);
    watermarker.onPartitionProcessBegin(part1, 0l, now + 2015l);

    Table table2 = localTestTable(dbName, "testTable2", true);
    Partition part2 = localTestPartition(table2, Lists.newArrayList("2016"));
    watermarker.onTableProcessBegin(table2, 0l);
    watermarker.onPartitionProcessBegin(part2, 0l, now + 16l);

    List<WorkUnit> workunits = Lists.newArrayList();
    watermarker.onGetWorkunitsEnd(workunits);

    Assert.assertEquals(watermarker.getPreviousHighWatermark(part1).getValue(), 0l);
    Assert.assertEquals(watermarker.getPreviousHighWatermark(table).getValue(), 0l);

    Assert.assertEquals(watermarker.getPreviousHighWatermark(part2).getValue(), 0l);
    Assert.assertEquals(watermarker.getPreviousHighWatermark(table2).getValue(), 0l);

    Assert.assertEquals(workunits.size(), 2);

    Assert.assertEquals(workunits.get(0).getPropAsBoolean(PartitionLevelWatermarker.IS_WATERMARK_WORKUNIT_KEY), true);
    Assert.assertEquals(workunits.get(1).getPropAsBoolean(PartitionLevelWatermarker.IS_WATERMARK_WORKUNIT_KEY), true);

    Collections.sort(workunits, new Comparator<WorkUnit>() {
      @Override
      public int compare(WorkUnit o1, WorkUnit o2) {
        return o1.getProp(ConfigurationKeys.DATASET_URN_KEY).compareTo(o2.getProp(ConfigurationKeys.DATASET_URN_KEY));
      }
    });

    Assert.assertEquals(workunits.get(0).getProp(ConfigurationKeys.DATASET_URN_KEY), table.getCompleteName());
    Assert.assertEquals(workunits.get(1).getProp(ConfigurationKeys.DATASET_URN_KEY), table2.getCompleteName());

    Assert.assertEquals(workunits.get(0).getExpectedHighWatermark(MultiKeyValueLongWatermark.class).getWatermarks(),
        ImmutableMap.of(PartitionLevelWatermarker.partitionKey(part1), now + 2015l));
    Assert.assertEquals(workunits.get(1).getExpectedHighWatermark(MultiKeyValueLongWatermark.class).getWatermarks(),
        ImmutableMap.of(PartitionLevelWatermarker.partitionKey(part2), now + 16l));
  }

  @Test
  public void testReadPreviousWatermarks() throws Exception {
    WorkUnitState previousWus = new WorkUnitState();
    previousWus.setProp(ConfigurationKeys.DATASET_URN_KEY, "test_dataset_urn");
    previousWus.setProp(PartitionLevelWatermarker.IS_WATERMARK_WORKUNIT_KEY, true);
    previousWus.setActualHighWatermark(new MultiKeyValueLongWatermark(ImmutableMap.of("2015", 100l, "2016", 101l)));

    SourceState state = new SourceState(new State(), Lists.newArrayList(previousWus));
    PartitionLevelWatermarker watermarker = new PartitionLevelWatermarker(state);

    Assert.assertEquals(watermarker.getPreviousWatermarks().size(), 1);
    Assert.assertEquals(watermarker.getPreviousWatermarks().get("test_dataset_urn"),
        ImmutableMap.of("2015", 100l, "2016", 101l));

    // Make sure all the previousWatermarks are added into current expectedHighWatermarks
    Assert.assertEquals(watermarker.getPreviousWatermarks(), watermarker.getExpectedHighWatermarks());

  }

  @Test
  public void testStateStoreReadWrite() throws Exception {

    String dbName = "testStateStoreReadWrite";
    LocalHiveMetastoreTestUtils.getInstance().dropDatabaseIfExists(dbName);

    PartitionLevelWatermarker watermarker0 = new PartitionLevelWatermarker(new SourceState());
    Table mockTable = localTestTable(dbName, "table1", true);

    watermarker0.onTableProcessBegin(mockTable, 0l);
    long now = new DateTime().getMillis();
    watermarker0.onPartitionProcessBegin(localTestPartition(mockTable, ImmutableList.of("2016")), 0, now);
    List<WorkUnit> workunits = Lists.newArrayList();
    watermarker0.onGetWorkunitsEnd(workunits);

    @SuppressWarnings("deprecation")
    WorkUnitState previousWus = new WorkUnitState(workunits.get(0));
    watermarker0.setActualHighWatermark(previousWus);

    SourceState state = new SourceState(new State(), Lists.newArrayList(previousWus));
    PartitionLevelWatermarker watermarker = new PartitionLevelWatermarker(state);

    Assert.assertEquals(watermarker.getPreviousWatermarks().size(), 1);
    Assert.assertEquals(watermarker.getPreviousWatermarks().get(dbName + "@table1"), ImmutableMap.of("2016", now));

  }

  @Test
  public void testReadPreviousWatermarksMultipleTables() throws Exception {
    WorkUnitState previousWus = new WorkUnitState();
    previousWus.setProp(ConfigurationKeys.DATASET_URN_KEY, "test_dataset_urn");
    previousWus.setProp(PartitionLevelWatermarker.IS_WATERMARK_WORKUNIT_KEY, true);
    previousWus.setActualHighWatermark(new MultiKeyValueLongWatermark(ImmutableMap.of("2015", 100l, "2016", 101l)));

    WorkUnitState previousWus2 = new WorkUnitState();
    previousWus2.setProp(ConfigurationKeys.DATASET_URN_KEY, "test_dataset_urn2");
    previousWus2.setProp(PartitionLevelWatermarker.IS_WATERMARK_WORKUNIT_KEY, true);
    previousWus2.setActualHighWatermark(new MultiKeyValueLongWatermark(ImmutableMap.of("01", 1l, "02", 2l)));

    SourceState state = new SourceState(new State(), Lists.newArrayList(previousWus, previousWus2));
    PartitionLevelWatermarker watermarker = new PartitionLevelWatermarker(state);

    Assert.assertEquals(watermarker.getPreviousWatermarks().size(), 2);
    Assert.assertEquals(watermarker.getPreviousWatermarks().get("test_dataset_urn"),
        ImmutableMap.of("2015", 100l, "2016", 101l));
    Assert.assertEquals(watermarker.getPreviousWatermarks().get("test_dataset_urn2"),
        ImmutableMap.of("01", 1l, "02", 2l));

    // Make sure all the previousWatermarks are added into current expectedHighWatermarks
    Assert.assertEquals(watermarker.getPreviousWatermarks(), watermarker.getExpectedHighWatermarks());

  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testMoreThanOneWatermarkWorkunits() throws Exception {
    WorkUnitState previousWus = new WorkUnitState();
    previousWus.setProp(ConfigurationKeys.DATASET_URN_KEY, "test_dataset_urn");
    previousWus.setProp(PartitionLevelWatermarker.IS_WATERMARK_WORKUNIT_KEY, true);
    previousWus.setActualHighWatermark(new MultiKeyValueLongWatermark(ImmutableMap.of("2015", 100l)));

    WorkUnitState previousWus2 = new WorkUnitState();
    previousWus2.setProp(ConfigurationKeys.DATASET_URN_KEY, "test_dataset_urn");
    previousWus2.setProp(PartitionLevelWatermarker.IS_WATERMARK_WORKUNIT_KEY, true);
    previousWus2.setActualHighWatermark(new MultiKeyValueLongWatermark(ImmutableMap.of("2016", 101l)));

    SourceState state = new SourceState(new State(), Lists.newArrayList(previousWus, previousWus2));

    // Expecting IllegalStateException
    new PartitionLevelWatermarker(state);

  }

  @Test
  public void testReadPreviousNullWatermarks() throws Exception {
    WorkUnitState previousWus = new WorkUnitState();
    previousWus.setProp(ConfigurationKeys.DATASET_URN_KEY, "test_dataset_urn");
    previousWus.setProp(PartitionLevelWatermarker.IS_WATERMARK_WORKUNIT_KEY, true);

    SourceState state = new SourceState(new State(), Lists.newArrayList(previousWus));
    PartitionLevelWatermarker watermarker = new PartitionLevelWatermarker(state);

    Assert.assertEquals(watermarker.getPreviousWatermarks().size(), 0);

  }

  @Test
  public void testNoPreviousWatermarkWorkunits() throws Exception {

    // Create one previous workunit with IS_WATERMARK_WORKUNIT_KEY set to true
    WorkUnitState previousWus = new WorkUnitState();
    previousWus.setProp(ConfigurationKeys.DATASET_URN_KEY, "test_dataset_urn");
    previousWus.setProp(PartitionLevelWatermarker.IS_WATERMARK_WORKUNIT_KEY, true);
    previousWus.setActualHighWatermark(new MultiKeyValueLongWatermark(ImmutableMap.of("2015", 100l)));

    // Create one previous workunit with IS_WATERMARK_WORKUNIT_KEY not set (false)
    WorkUnitState previousWus2 = new WorkUnitState();
    previousWus2.setProp(ConfigurationKeys.DATASET_URN_KEY, "test_dataset_urn2");
    previousWus2.setActualHighWatermark(new LongWatermark(101l));

    SourceState state = new SourceState(new State(), Lists.newArrayList(previousWus, previousWus2));
    PartitionLevelWatermarker watermarker = new PartitionLevelWatermarker(state);

    Assert.assertEquals(watermarker.getPreviousWatermarks().size(), 1);
    Assert.assertEquals(watermarker.getPreviousWatermarks().get("test_dataset_urn"), ImmutableMap.of("2015", 100l));

  }

  @Test
  public void testGetPreviousHighWatermarkForPartition() throws Exception {
    WorkUnitState previousWus = new WorkUnitState();
    previousWus.setProp(ConfigurationKeys.DATASET_URN_KEY, "db@test_dataset_urn");
    previousWus.setProp(PartitionLevelWatermarker.IS_WATERMARK_WORKUNIT_KEY, true);
    previousWus.setActualHighWatermark(new MultiKeyValueLongWatermark(ImmutableMap.of("2015", 100l, "2016", 101l)));

    SourceState state = new SourceState(new State(), Lists.newArrayList(previousWus));
    PartitionLevelWatermarker watermarker = new PartitionLevelWatermarker(state);

    Table table = mockTable("test_dataset_urn");
    Partition partition2015 = mockPartition(table, ImmutableList.of("2015"));
    Partition partition2016 = mockPartition(table, ImmutableList.of("2016"));

    Assert.assertEquals(watermarker.getPreviousHighWatermark(partition2015), new LongWatermark(100l));
    Assert.assertEquals(watermarker.getPreviousHighWatermark(partition2016), new LongWatermark(101l));
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testPartitionBeginBegoreTableBegin() throws Exception {
    SourceState state = new SourceState();
    PartitionLevelWatermarker watermarker = new PartitionLevelWatermarker(state);

    Table table = mockTable("test_dataset_urn");
    Partition partition = mockPartition(table, ImmutableList.of(""));

    watermarker.onPartitionProcessBegin(partition, 0l, 0l);
  }

  @Test
  public void testDroppedPartitions() throws Exception {
    WorkUnitState previousWus = new WorkUnitState();
    previousWus.setProp(ConfigurationKeys.DATASET_URN_KEY, "db@test_dataset_urn");
    previousWus.setProp(PartitionLevelWatermarker.IS_WATERMARK_WORKUNIT_KEY, true);
    previousWus
        .setActualHighWatermark(new MultiKeyValueLongWatermark(ImmutableMap.of("2015-01", 100l, "2015-02", 101l)));

    SourceState state = new SourceState(new State(), Lists.newArrayList(previousWus));
    PartitionLevelWatermarker watermarker = new PartitionLevelWatermarker(state);

    Table table = mockTable("test_dataset_urn");
    Mockito.when(table.getPartitionKeys()).thenReturn(ImmutableList.of(new FieldSchema("year", "string", "")));

    Partition partition2015 = mockPartition(table, ImmutableList.of("2015"));

    // partition 2015 replaces 2015-01 and 2015-02
    Mockito.when(partition2015.getParameters()).thenReturn(
        ImmutableMap.of(AbstractAvroToOrcConverter.REPLACED_PARTITIONS_HIVE_METASTORE_KEY, "2015-01|2015-02"));
    watermarker.onPartitionProcessBegin(partition2015, 0l, 0l);

    Assert.assertEquals(watermarker.getExpectedHighWatermarks().get("db@test_dataset_urn"), ImmutableMap.of("2015", 0l));
  }

  // No previous state. 5 new modified partitions. Only 3 most recently modified retained in getExpectedHighWatermark
  @Test
  public void testRecentlyModifiedPartitionWatermarks() throws Exception {

    String dbName = "testRecentlyModifiedPartitionWatermarks";
    LocalHiveMetastoreTestUtils.getInstance().dropDatabaseIfExists(dbName);

    SourceState state = new SourceState();
    state.setProp(HiveSource.HIVE_SOURCE_MAXIMUM_LOOKBACK_DAYS_KEY, 3);
    long time5DaysAgo = new DateTime().minusDays(5).getMillis();

    PartitionLevelWatermarker watermarker = new PartitionLevelWatermarker(state);
    watermarker.setLeastWatermarkToPersistInState(time5DaysAgo);

    Table table = localTestTable(dbName, "testTable2", true);
    Partition part2010 = localTestPartition(table, ImmutableList.of("2010"));
    Partition part2011 = localTestPartition(table, ImmutableList.of("2011"));
    Partition part2012 = localTestPartition(table, ImmutableList.of("2012"));
    Partition part2013 = localTestPartition(table, ImmutableList.of("2013"));
    Partition part2014 = localTestPartition(table, ImmutableList.of("2014"));

    watermarker.onTableProcessBegin(table, 0l);

    watermarker.onPartitionProcessBegin(part2010, 0l, time5DaysAgo - 100l);
    watermarker.onPartitionProcessBegin(part2011, 0l, time5DaysAgo - 101l);

    watermarker.onPartitionProcessBegin(part2012, 0l, time5DaysAgo + 102l);
    watermarker.onPartitionProcessBegin(part2013, 0l, time5DaysAgo + 103l);
    watermarker.onPartitionProcessBegin(part2014, 0l, time5DaysAgo + 104l);

    List<WorkUnit> workunits = Lists.newArrayList();
    watermarker.onGetWorkunitsEnd(workunits);

    Assert.assertEquals(workunits.size(), 1);
    WorkUnit watermarkWu = workunits.get(0);

    Map<String, Long> workunitWatermarks =
        watermarkWu.getExpectedHighWatermark(MultiKeyValueLongWatermark.class).getWatermarks();
    Assert.assertEquals(workunitWatermarks.size(), 3, "expectedHighWatermarks size");

    ImmutableMap<String, Long> expectedWatermarks =
        ImmutableMap.of("2014", time5DaysAgo + 104l, "2013", time5DaysAgo + 103l, "2012", time5DaysAgo + 102l);
    Assert.assertEquals(workunitWatermarks, expectedWatermarks);

  }

  //Previous state 3. New partitions 3. 2 from new state retained
  @Test
  public void testRecentlyModifiedPartitionWatermarksWithPreviousState() throws Exception {

    String dbName = "testRecentlyModifiedPartitionWatermarksWithPreviousState";
    LocalHiveMetastoreTestUtils.getInstance().dropDatabaseIfExists(dbName);

    long time5DaysAgo = new DateTime().minusDays(5).getMillis();

    WorkUnitState previousWus = new WorkUnitState();
    previousWus.setProp(ConfigurationKeys.DATASET_URN_KEY, dbName + "@testTable2");
    previousWus.setProp(PartitionLevelWatermarker.IS_WATERMARK_WORKUNIT_KEY, true);
    previousWus.setActualHighWatermark(new MultiKeyValueLongWatermark(ImmutableMap.of("2010", time5DaysAgo - 100l, // Do not retain
        "2011", time5DaysAgo - 101l, // Do not retain
        "2012", time5DaysAgo + 102l // Do retain
        )));

    SourceState state = new SourceState(new State(), Lists.newArrayList(previousWus));
    state.setProp(HiveSource.HIVE_SOURCE_MAXIMUM_LOOKBACK_DAYS_KEY, 3);

    PartitionLevelWatermarker watermarker = new PartitionLevelWatermarker(state);
    watermarker.setLeastWatermarkToPersistInState(time5DaysAgo);

    Table table = localTestTable(dbName, "testTable2", true);

    // Watermark not retained
    Partition part2009 = localTestPartition(table, ImmutableList.of("2009"));

    // Watermark retained
    Partition part2013 = localTestPartition(table, ImmutableList.of("2013"));
    Partition part2014 = localTestPartition(table, ImmutableList.of("2014"));

    watermarker.onTableProcessBegin(table, 0l);

    // Watermark not retained
    watermarker.onPartitionProcessBegin(part2009, 0l, time5DaysAgo - 99l);

    // Watermark retained
    watermarker.onPartitionProcessBegin(part2013, 0l, time5DaysAgo + 103l);
    watermarker.onPartitionProcessBegin(part2014, 0l, time5DaysAgo + 104l);

    List<WorkUnit> workunits = Lists.newArrayList();
    watermarker.onGetWorkunitsEnd(workunits);

    Assert.assertEquals(workunits.size(), 1);
    WorkUnit watermarkWu = workunits.get(0);

    Map<String, Long> workunitWatermarks =
        watermarkWu.getExpectedHighWatermark(MultiKeyValueLongWatermark.class).getWatermarks();
    Assert.assertEquals(workunitWatermarks.size(), 3, "expectedHighWatermarks size");

    ImmutableMap<String, Long> expectedWatermarks =
        ImmutableMap.of("2014", time5DaysAgo + 104l, "2013", time5DaysAgo + 103l, "2012", time5DaysAgo + 102l);
    Assert.assertEquals(workunitWatermarks, expectedWatermarks);

  }

  private static Table mockTable(String name) {
    Table table = Mockito.mock(Table.class, Mockito.RETURNS_SMART_NULLS);
    Mockito.when(table.getCompleteName()).thenReturn("db@" + name);
    return table;
  }

  private static Partition mockPartition(Table table, List<String> values) {
    Partition partition = Mockito.mock(Partition.class, Mockito.RETURNS_SMART_NULLS);
    Mockito.when(partition.getTable()).thenReturn(table);
    Mockito.when(partition.getValues()).thenReturn(values);
    return partition;
  }

  private static Table localTestTable(String dbName, String name, boolean partitioned) throws Exception {
    File tableSdFile = Files.createTempDir();
    tableSdFile.deleteOnExit();
    return new Table(LocalHiveMetastoreTestUtils.getInstance()
        .createTestTable(dbName, name, tableSdFile.getAbsolutePath(),
            partitioned ? Optional.of("part") : Optional.<String>absent()));
  }

  private static Partition localTestPartition(Table table, List<String> values) throws Exception {
    return new Partition(table,
        LocalHiveMetastoreTestUtils.getInstance().addTestPartition(table.getTTable(), values, 0));
  }
}
