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
package gobblin.data.management.conversion.hive;

import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.SourceState;
import gobblin.configuration.WorkUnitState;
import gobblin.data.management.conversion.hive.mock.MockUpdateProvider;
import gobblin.data.management.conversion.hive.mock.MockUpdateProviderFactory;
import gobblin.hive.HiveMetastoreClientPool;
import gobblin.hive.HivePartition;
import gobblin.hive.HiveRegistrationUnit;
import gobblin.hive.HiveTable;
import gobblin.source.extractor.extract.LongWatermark;
import gobblin.source.workunit.WorkUnit;


@Slf4j
@Test(groups = { "gobblin.data.management.conversion" })
public class HiveSourceTest {

  private IMetaStoreClient localMetastoreClient;
  private HiveSource hiveSource;
  private MockUpdateProvider updateProvider;

  @BeforeClass
  public void setup() throws Exception {
    this.localMetastoreClient =
        HiveMetastoreClientPool.get(new Properties(), Optional.<String> absent()).getClient().get();
    this.hiveSource = new HiveSource();
    this.updateProvider = MockUpdateProvider.getInstance();
  }

  @Test
  public void testGetWorkUnitsForTable() throws Exception {

    String dbName = "testdb2";
    String tableName = "testtable2";
    String tableSdLoc = "/tmp/testtable2";

    this.localMetastoreClient.dropDatabase(dbName, false, true, true);

    SourceState testState = getTestState(dbName);

    createTestTable(dbName, tableName, tableSdLoc, Optional.<String> absent());

    this.updateProvider.addMockUpdateTime(dbName + "@" + tableName, 10);

    List<WorkUnit> workUnits = this.hiveSource.getWorkunits(testState);

    Assert.assertEquals(workUnits.size(), 1);
    WorkUnit wu = workUnits.get(0);
    HiveRegistrationUnit hiveUnit = HiveSource.GENERICS_AWARE_GSON
        .fromJson(wu.getProp(HiveSource.HIVE_UNIT_SERIALIZED_KEY), HiveRegistrationUnit.class);
    Assert.assertTrue(hiveUnit instanceof HiveTable, "Serialized hive unit is not a table");
    Assert.assertEquals(hiveUnit.getDbName(), dbName);
    Assert.assertEquals(hiveUnit.getTableName(), tableName);
    Assert.assertEquals(hiveUnit.getLocation().get(), "file:" + tableSdLoc);

  }

  @Test
  public void testGetWorkUnitsForPartitions() throws Exception {

    String dbName = "testdb3";
    String tableName = "testtable3";
    String tableSdLoc = "/tmp/testtable3";

    this.localMetastoreClient.dropDatabase(dbName, false, true, true);

    SourceState testState = getTestState(dbName);

    Table tbl = createTestTable(dbName, tableName, tableSdLoc, Optional.of("field"));

    Partition partition = addTestPartition(tbl, ImmutableList.of("f1"));

    this.updateProvider.addMockUpdateTime("testdb3@testtable3@field=f1", 2);

    List<WorkUnit> workUnits = this.hiveSource.getWorkunits(testState);

    Assert.assertEquals(workUnits.size(), 1);
    WorkUnit wu = workUnits.get(0);
    HiveRegistrationUnit hiveUnit = HiveSource.GENERICS_AWARE_GSON
        .fromJson(wu.getProp(HiveSource.HIVE_UNIT_SERIALIZED_KEY), HiveRegistrationUnit.class);

    Assert.assertEquals(hiveUnit.getDbName(), dbName);
    Assert.assertEquals(hiveUnit.getTableName(), tableName);
    Assert.assertEquals(hiveUnit.getLocation().get(), partition.getSd().getLocation());

    Assert.assertTrue(hiveUnit instanceof HivePartition, "Serialized hive unit is not a partition");
    HivePartition hivePartition = (HivePartition) hiveUnit;
    Assert.assertEquals(hivePartition.getValues().get(0), "f1");

  }

  @Test
  public void testGetWorkunitsAfterWatermark() throws Exception {

    String dbName = "testdb4";
    String tableName1 = "testtable1";
    String tableSdLoc1 = "/tmp/testtable1";
    String tableName2 = "testtable2";
    String tableSdLoc2 = "/tmp/testtable2";

    this.localMetastoreClient.dropDatabase(dbName, false, true, true);

    List<WorkUnitState> previousWorkUnitStates = Lists.newArrayList();
    previousWorkUnitStates.add(createPreviousWus(dbName, tableName1, 10));
    previousWorkUnitStates.add(createPreviousWus(dbName, tableName2, 10));

    SourceState testState = new SourceState(getTestState(dbName), previousWorkUnitStates);

    createTestTable(dbName, tableName1, tableSdLoc1, Optional.<String> absent());
    createTestTable(dbName, tableName2, tableSdLoc2, Optional.<String> absent(), true);

    this.updateProvider.addMockUpdateTime(dbName + "@" + tableName1, 10);
    this.updateProvider.addMockUpdateTime(dbName + "@" + tableName2, 15);

    List<WorkUnit> workUnits = this.hiveSource.getWorkunits(testState);

    Assert.assertEquals(workUnits.size(), 1);
    WorkUnit wu = workUnits.get(0);
    HiveRegistrationUnit hiveUnit = HiveSource.GENERICS_AWARE_GSON
        .fromJson(wu.getProp(HiveSource.HIVE_UNIT_SERIALIZED_KEY), HiveRegistrationUnit.class);
    Assert.assertTrue(hiveUnit instanceof HiveTable, "Serialized hive unit is not a table");
    Assert.assertEquals(hiveUnit.getDbName(), dbName);
    Assert.assertEquals(hiveUnit.getTableName(), tableName2);

  }

  private static WorkUnitState createPreviousWus(String dbName, String tableName, long watermark) {

    WorkUnitState wus = new WorkUnitState();
    wus.setActualHighWatermark(new LongWatermark(watermark));
    wus.setProp(ConfigurationKeys.DATASET_URN_KEY, dbName + "@" + tableName);

    return wus;
  }

  private Table createTestTable(String dbName, String tableName, String tableSdLoc, Optional<String> partitionFieldName)
      throws Exception {
    return createTestTable(dbName, tableName, tableSdLoc, partitionFieldName, false);
  }

  private Table createTestTable(String dbName, String tableName, String tableSdLoc, Optional<String> partitionFieldName,
      boolean ignoreDbCreation) throws Exception {
    if (!ignoreDbCreation) {
      createTestDb(dbName);
    }
    Table tbl = org.apache.hadoop.hive.ql.metadata.Table.getEmptyTable(dbName, tableName);
    tbl.getSd().setLocation(tableSdLoc);
    if (partitionFieldName.isPresent()) {
      tbl.addToPartitionKeys(new FieldSchema(partitionFieldName.get(), "string", "some comment"));
    }
    this.localMetastoreClient.createTable(tbl);

    return tbl;
  }

  private void createTestDb(String dbName) throws Exception {

    Database db = new Database(dbName, "Some description", "/tmp/" + dbName, new HashMap<String, String>());
    try {
      this.localMetastoreClient.createDatabase(db);
    } catch (AlreadyExistsException e) {
      log.warn(dbName + " already exits");
    }

  }

  private static SourceState getTestState(String dbName) {
    SourceState testState = new SourceState();
    testState.setProp("hive.dataset.database", dbName);
    testState.setProp("hive.dataset.table.pattern", "*");
    testState.setProp("hive.unit.updateProviderFactory.class", MockUpdateProviderFactory.class.getCanonicalName());
    return testState;
  }

  private Partition addTestPartition(Table tbl, List<String> values) throws Exception {
    StorageDescriptor partitionSd = new StorageDescriptor();
    partitionSd.setLocation("/tmp/" + tbl.getTableName() + "/part1");
    partitionSd.setSerdeInfo(new SerDeInfo("name", "serializationLib", new HashMap<String, String>()));
    partitionSd.setCols(tbl.getPartitionKeys());

    Partition partition =
        new Partition(values, tbl.getDbName(), tbl.getTableName(), 1, 1, partitionSd, new HashMap<String, String>());

    this.localMetastoreClient.add_partition(partition);

    return partition;
  }

}
