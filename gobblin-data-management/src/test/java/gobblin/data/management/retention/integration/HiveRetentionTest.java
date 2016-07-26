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
package gobblin.data.management.retention.integration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.joda.time.DateTimeUtils;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;

import gobblin.configuration.ConfigurationKeys;
import gobblin.data.management.conversion.hive.LocalHiveMetastoreTestUtils;
import gobblin.util.PathUtils;
import gobblin.util.test.RetentionTestDataGenerator.FixedThreadLocalMillisProvider;
import gobblin.util.test.RetentionTestHelper;


public class HiveRetentionTest {

  private FileSystem fs;
  private Path testTempPath;

  private LocalHiveMetastoreTestUtils hiveMetastoreTestUtils;
  private static final DateTimeFormatter FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd-HH").withZone(
      DateTimeZone.forID(ConfigurationKeys.PST_TIMEZONE_NAME));

  @BeforeClass
  public void setup() throws Exception {
    this.hiveMetastoreTestUtils = LocalHiveMetastoreTestUtils.getInstance();
    fs = FileSystem.getLocal(new Configuration());
    testTempPath = new Path(this.getClass().getClassLoader().getResource("").getFile(), "HiveRetentionTest");
    fs.mkdirs(testTempPath);
  }

  private void testTimeBasedHiveRetention(String dbName, String tableName, String configFileName) throws Exception {

    DateTimeUtils.setCurrentMillisProvider(new FixedThreadLocalMillisProvider(FORMATTER.parseDateTime("2016-01-10-00").getMillis()));

    // Setup 4 partitions. 2 will be deleted and 2 will be retained
    String tableSdLoc = new Path(testTempPath, dbName + tableName).toString();
    this.hiveMetastoreTestUtils.dropDatabaseIfExists(dbName);
    final Table tbl = this.hiveMetastoreTestUtils.createTestTable(dbName, tableName, tableSdLoc, ImmutableList.of("datepartition"), false);

    String deleted1 = "2016-01-01-00";
    String deleted2 = "2016-01-02-02";

    String retained1 = "2016-01-03-04";
    String retained2 = "2016-01-07-06";

    Partition pDeleted1 = this.hiveMetastoreTestUtils.addTestPartition(tbl, ImmutableList.of(deleted1), (int) System.currentTimeMillis());
    Partition pDeleted2 = this.hiveMetastoreTestUtils.addTestPartition(tbl, ImmutableList.of(deleted2), (int) System.currentTimeMillis());
    Partition pRetained1 = this.hiveMetastoreTestUtils.addTestPartition(tbl, ImmutableList.of(retained1), (int) System.currentTimeMillis());
    Partition pRetained2 = this.hiveMetastoreTestUtils.addTestPartition(tbl, ImmutableList.of(retained2), (int) System.currentTimeMillis());

    this.fs.mkdirs(new Path(pDeleted1.getSd().getLocation()));
    this.fs.mkdirs(new Path(pDeleted2.getSd().getLocation()));
    this.fs.mkdirs(new Path(pRetained1.getSd().getLocation()));
    this.fs.mkdirs(new Path(pRetained2.getSd().getLocation()));

    List<Partition> partitions = this.hiveMetastoreTestUtils.getLocalMetastoreClient().listPartitions(dbName, tableName, (short) 10);
    Assert.assertEquals(partitions.size(), 4);

    RetentionTestHelper.clean(fs, PathUtils.combinePaths(RetentionIntegrationTest.TEST_PACKAGE_RESOURCE_NAME, "testHiveTimeBasedRetention", configFileName),
        Optional.of(PathUtils.combinePaths(RetentionIntegrationTest.TEST_PACKAGE_RESOURCE_NAME, "testHiveTimeBasedRetention", "jobProps.properties")),
        testTempPath);

    partitions = this.hiveMetastoreTestUtils.getLocalMetastoreClient().listPartitions(dbName, tableName, (short) 10);

    Assert.assertEquals(partitions.size(), 2);

    assertThat(FluentIterable.from(partitions).transform(new Function<Partition, String>() {

      @Override
      public String apply(Partition input) {
        return getQlPartition(tbl, input).getName();
      }
    }).toList(), containsInAnyOrder(getQlPartition(tbl, pRetained1).getName(), getQlPartition(tbl, pRetained2).getName()));


    Assert.assertTrue(this.fs.exists(new Path(pRetained1.getSd().getLocation())));
    Assert.assertTrue(this.fs.exists(new Path(pRetained2.getSd().getLocation())));
    Assert.assertFalse(this.fs.exists(new Path(pDeleted1.getSd().getLocation())));
    Assert.assertFalse(this.fs.exists(new Path(pDeleted2.getSd().getLocation())));

    DateTimeUtils.setCurrentMillisSystem();
  }


  @Test(dependsOnMethods = { "testTimeBasedHiveRetentionWithJobProps" })
  public void testTimeBasedHiveRetentionWithConfigStore() throws Exception {
    String dbName = "hiveTestDbConfigStore";
    String tableName = "testTable";
    this.testTimeBasedHiveRetention(dbName, tableName, "selection.conf");
  }

  @Test
  public void testTimeBasedHiveRetentionWithJobProps() throws Exception {
    String dbName = "hiveTestDb";
    String tableName = "testTable";
    this.testTimeBasedHiveRetention(dbName, tableName, "hive-retention.job");
  }

  private static org.apache.hadoop.hive.ql.metadata.Partition getQlPartition(final Table table, final Partition partition) {
    try {
      return new org.apache.hadoop.hive.ql.metadata.Partition(new org.apache.hadoop.hive.ql.metadata.Table(table), partition);
    } catch (HiveException e) {
      throw new RuntimeException(e);
    }
  }

  @AfterClass
  public void cleanUp() {
    try {
      fs.delete(this.testTempPath, true);
    } catch (Exception e) {
      // ignore
    }
  }

}
