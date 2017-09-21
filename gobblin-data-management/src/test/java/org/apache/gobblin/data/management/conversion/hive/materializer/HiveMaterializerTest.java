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

package org.apache.gobblin.data.management.conversion.hive.materializer;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.data.management.conversion.hive.LocalHiveMetastoreTestUtils;
import org.apache.gobblin.data.management.conversion.hive.entities.TableLikeStageableTableMetadata;
import org.apache.gobblin.data.management.conversion.hive.task.HiveConverterUtils;
import org.apache.gobblin.data.management.conversion.hive.task.HiveTask;
import org.apache.gobblin.data.management.copy.hive.HiveDataset;
import org.apache.gobblin.hive.HiveMetastoreClientPool;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.runtime.TaskContext;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.util.AutoReturnableObject;
import org.apache.gobblin.util.HiveJdbcConnector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.io.Files;


public class HiveMaterializerTest {

  private final LocalHiveMetastoreTestUtils localHiveMetastore = LocalHiveMetastoreTestUtils.getInstance();
  private final String dbName = HiveMaterializerTest.class.getSimpleName();
  private final String sourceTableName = "source";
  private final String partitionColumn = "part";
  private File dataFile;
  private HiveJdbcConnector jdbcConnector;
  private HiveDataset dataset;
  private HiveMetastoreClientPool pool;

  @BeforeClass
  public void setup() throws Exception {
    this.jdbcConnector = HiveJdbcConnector.newEmbeddedConnector(2);
    this.dataFile = new File(getClass().getClassLoader().getResource("hiveMaterializerTest/source/").toURI());
    this.localHiveMetastore.dropDatabaseIfExists(this.dbName);
    this.localHiveMetastore.createTestDb(this.dbName);
    this.jdbcConnector.executeStatements(
        String.format("CREATE EXTERNAL TABLE %s.%s (id STRING, name String) PARTITIONED BY (%s String) "
                + "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE",
            this.dbName, this.sourceTableName, this.partitionColumn),
        String.format("ALTER TABLE %s.%s ADD PARTITION (part = 'part1') LOCATION '%s'",
            this.dbName, this.sourceTableName, this.dataFile.getAbsolutePath() + "/part1"),
        String.format("ALTER TABLE %s.%s ADD PARTITION (part = 'part2') LOCATION '%s'",
            this.dbName, this.sourceTableName, this.dataFile.getAbsolutePath() + "/part2"));

    List<List<String>> allTable = executeStatementAndGetResults(this.jdbcConnector,
        String.format("SELECT * FROM %s.%s", this.dbName, this.sourceTableName), 3);
    Assert.assertEquals(allTable.size(), 8);
    List<List<String>> part1 = executeStatementAndGetResults(this.jdbcConnector,
        String.format("SELECT * FROM %s.%s WHERE %s='part1'", this.dbName, this.sourceTableName, this.partitionColumn), 3);
    Assert.assertEquals(part1.size(), 4);

    this.pool = HiveMetastoreClientPool.get(new Properties(), Optional.absent());
    Table table;
    try (AutoReturnableObject<IMetaStoreClient> client = pool.getClient()) {
      table = new Table(client.get().getTable(this.dbName, this.sourceTableName));
    }
    this.dataset = new HiveDataset(FileSystem.getLocal(new Configuration()), pool, table, new Properties());
  }

  @AfterClass
  public void teardown() throws Exception {
    if (this.jdbcConnector != null) {
      this.jdbcConnector.close();
    }
  }

  @Test
  public void testCopyTable() throws Exception {
    String destinationTable = "copyTable";
    File tmpDir = Files.createTempDir();
    tmpDir.deleteOnExit();
    WorkUnit workUnit = HiveMaterializer.tableCopyWorkUnit(this.dataset, new TableLikeStageableTableMetadata(this.dataset.getTable(),
        this.dbName, destinationTable, tmpDir.getAbsolutePath()), String.format("%s=part1", this.partitionColumn));

    HiveMaterializer hiveMaterializer = new HiveMaterializer(getTaskContextForRun(workUnit));
    hiveMaterializer.run();
    Assert.assertEquals(hiveMaterializer.getWorkingState(), WorkUnitState.WorkingState.SUCCESSFUL);
    hiveMaterializer.commit();
    Assert.assertEquals(hiveMaterializer.getWorkingState(), WorkUnitState.WorkingState.SUCCESSFUL);

    List<List<String>> allTable = executeStatementAndGetResults(this.jdbcConnector,
        String.format("SELECT * FROM %s.%s", this.dbName, destinationTable), 3);
    Assert.assertEquals(allTable.size(), 4);
    Assert.assertEquals(allTable.stream().map(l -> l.get(0)).collect(Collectors.toList()), Lists.newArrayList("101", "102", "103", "104"));
  }

  @Test
  public void testMaterializeTable() throws Exception {
    String destinationTable = "materializeTable";
    File tmpDir = Files.createTempDir();
    tmpDir.deleteOnExit();
    WorkUnit workUnit = HiveMaterializer.viewMaterializationWorkUnit(this.dataset, HiveConverterUtils.StorageFormat.AVRO,
        new TableLikeStageableTableMetadata(this.dataset.getTable(), this.dbName, destinationTable, tmpDir.getAbsolutePath()), null);

    HiveMaterializer hiveMaterializer = new HiveMaterializer(getTaskContextForRun(workUnit));
    hiveMaterializer.run();
    Assert.assertEquals(hiveMaterializer.getWorkingState(), WorkUnitState.WorkingState.SUCCESSFUL);
    hiveMaterializer.commit();
    Assert.assertEquals(hiveMaterializer.getWorkingState(), WorkUnitState.WorkingState.SUCCESSFUL);

    List<List<String>> allTable = executeStatementAndGetResults(this.jdbcConnector,
        String.format("SELECT * FROM %s.%s", this.dbName, destinationTable), 3);
    Assert.assertEquals(allTable.size(), 8);
    Assert.assertEquals(allTable.stream().map(l -> l.get(0)).collect(Collectors.toList()),
        Lists.newArrayList("101", "102", "103", "104", "201", "202", "203", "204"));
  }

  @Test
  public void testMaterializeTablePartition() throws Exception {
    String destinationTable = "materializeTablePartition";
    File tmpDir = Files.createTempDir();
    tmpDir.deleteOnExit();
    WorkUnit workUnit = HiveMaterializer.viewMaterializationWorkUnit(this.dataset, HiveConverterUtils.StorageFormat.AVRO,
        new TableLikeStageableTableMetadata(this.dataset.getTable(), this.dbName, destinationTable, tmpDir.getAbsolutePath()),
        String.format("%s=part1", this.partitionColumn));

    HiveMaterializer hiveMaterializer = new HiveMaterializer(getTaskContextForRun(workUnit));
    hiveMaterializer.run();
    Assert.assertEquals(hiveMaterializer.getWorkingState(), WorkUnitState.WorkingState.SUCCESSFUL);
    hiveMaterializer.commit();
    Assert.assertEquals(hiveMaterializer.getWorkingState(), WorkUnitState.WorkingState.SUCCESSFUL);

    List<List<String>> allTable = executeStatementAndGetResults(this.jdbcConnector,
        String.format("SELECT * FROM %s.%s", this.dbName, destinationTable), 3);
    Assert.assertEquals(allTable.size(), 4);
    Assert.assertEquals(allTable.stream().map(l -> l.get(0)).collect(Collectors.toList()),
        Lists.newArrayList("101", "102", "103", "104"));
  }

  @Test
  public void testMaterializeView() throws Exception {
    String destinationTable = "materializeView";
    File tmpDir = Files.createTempDir();
    tmpDir.deleteOnExit();

    String viewName = "myView";

    this.jdbcConnector.executeStatements(String.format("CREATE VIEW %s.%s AS SELECT * FROM %s.%s WHERE name = 'foo'",
        this.dbName, viewName, this.dbName, this.sourceTableName));

    Table view;
    try (AutoReturnableObject<IMetaStoreClient> client = pool.getClient()) {
      view = new Table(client.get().getTable(this.dbName, viewName));
    }
    HiveDataset viewDataset = new HiveDataset(FileSystem.getLocal(new Configuration()), pool, view, new Properties());

    WorkUnit workUnit = HiveMaterializer.viewMaterializationWorkUnit(viewDataset, HiveConverterUtils.StorageFormat.AVRO,
        new TableLikeStageableTableMetadata(viewDataset.getTable(), this.dbName, destinationTable, tmpDir.getAbsolutePath()),
        null);

    HiveMaterializer hiveMaterializer = new HiveMaterializer(getTaskContextForRun(workUnit));
    hiveMaterializer.run();
    Assert.assertEquals(hiveMaterializer.getWorkingState(), WorkUnitState.WorkingState.SUCCESSFUL);
    hiveMaterializer.commit();
    Assert.assertEquals(hiveMaterializer.getWorkingState(), WorkUnitState.WorkingState.SUCCESSFUL);

    List<List<String>> allTable = executeStatementAndGetResults(this.jdbcConnector,
        String.format("SELECT * FROM %s.%s", this.dbName, destinationTable), 3);
    Assert.assertEquals(allTable.size(), 4);
    Assert.assertEquals(allTable.stream().map(l -> l.get(0)).collect(Collectors.toList()),
        Lists.newArrayList("101", "103", "201", "203"));
  }

  @Test
  public void testMaterializeQuery() throws Exception {
    String destinationTable = "materializeQuery";
    File tmpDir = Files.createTempDir();
    tmpDir.deleteOnExit();

    WorkUnit workUnit = HiveMaterializer.queryResultMaterializationWorkUnit(
        String.format("SELECT * FROM %s.%s WHERE name = 'foo'", this.dbName, this.sourceTableName),
        HiveConverterUtils.StorageFormat.AVRO,
        new TableLikeStageableTableMetadata(this.dataset.getTable(), this.dbName, destinationTable, tmpDir.getAbsolutePath()));

    HiveMaterializer hiveMaterializer = new HiveMaterializer(getTaskContextForRun(workUnit));
    hiveMaterializer.run();
    Assert.assertEquals(hiveMaterializer.getWorkingState(), WorkUnitState.WorkingState.SUCCESSFUL);
    hiveMaterializer.commit();
    Assert.assertEquals(hiveMaterializer.getWorkingState(), WorkUnitState.WorkingState.SUCCESSFUL);

    List<List<String>> allTable = executeStatementAndGetResults(this.jdbcConnector,
        String.format("SELECT * FROM %s.%s", this.dbName, destinationTable), 3);
    Assert.assertEquals(allTable.size(), 4);
    Assert.assertEquals(allTable.stream().map(l -> l.get(0)).collect(Collectors.toList()),
        Lists.newArrayList("101", "103", "201", "203"));
  }

  private TaskContext getTaskContextForRun(WorkUnit workUnit) {
    workUnit.setProp(ConfigurationKeys.JOB_ID_KEY, "job123");
    workUnit.setProp(ConfigurationKeys.TASK_ID_KEY, "task123");
    workUnit.setProp(HiveConverterUtils.HIVE_DATASET_DESTINATION_SKIP_SETGROUP, Boolean.toString(true));
    HiveTask.disableHiveWatermarker(workUnit);
    JobState jobState = new JobState("job", "job123");
    return new TaskContext(new WorkUnitState(workUnit, jobState));
  }

  private List<List<String>> executeStatementAndGetResults(HiveJdbcConnector connector, String query, int columns) throws SQLException {
    Connection conn = connector.getConnection();
    List<List<String>> result = new ArrayList<>();

    try (Statement stmt = conn.createStatement()) {
      stmt.execute(query);
      ResultSet rs = stmt.getResultSet();
      while (rs.next()) {
        List<String> thisResult = new ArrayList<>();
        for (int i = 0; i < columns; i++) {
          thisResult.add(rs.getString(i + 1));
        }
        result.add(thisResult);
      }
    }
    return result;
  }

}
