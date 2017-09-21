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
package org.apache.gobblin.data.management.retention.integration;

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
import com.google.common.io.Files;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.data.management.conversion.hive.LocalHiveMetastoreTestUtils;
import org.apache.gobblin.util.PathUtils;
import org.apache.gobblin.util.test.RetentionTestDataGenerator.FixedThreadLocalMillisProvider;
import org.apache.gobblin.util.test.RetentionTestHelper;

@Test(groups = { "SystemTimeTests"})
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
    testTempPath = new Path(Files.createTempDir().getAbsolutePath(), "HiveRetentionTest");
    fs.mkdirs(testTempPath);
  }

  private void testTimeBasedHiveRetention(String purgedDbName, String purgedTableName, String configFileName, boolean isReplacementTest) throws Exception {

    try {
      DateTimeUtils.setCurrentMillisProvider(new FixedThreadLocalMillisProvider(FORMATTER.parseDateTime("2016-01-10-00").getMillis()));

      // Setup db, table to purge. Creating 4 partitions. 2 will be deleted and 2 will be retained
      String purgedTableSdLoc = new Path(testTempPath, purgedDbName + purgedTableName).toString();
      this.hiveMetastoreTestUtils.dropDatabaseIfExists(purgedDbName);
      final Table purgedTbl = this.hiveMetastoreTestUtils.createTestAvroTable(purgedDbName, purgedTableName, purgedTableSdLoc, ImmutableList.of("datepartition"), false);

      // Setup db, table and partitions to act as replacement partitions source
      String replacementSourceTableSdLoc = new Path(testTempPath, purgedDbName + purgedTableName + "_source").toString();
      String replacementDbName = purgedDbName + "_source";
      String replacementTableName = purgedTableName + "_source";
      this.hiveMetastoreTestUtils.dropDatabaseIfExists(replacementDbName);
      final Table replacementTbl = this.hiveMetastoreTestUtils.createTestAvroTable(replacementDbName, replacementTableName, replacementSourceTableSdLoc, ImmutableList.of("datepartition"), false);

      String deleted1 = "2016-01-01-00";
      String deleted2 = "2016-01-02-02";

      String retained1 = "2016-01-03-04";
      String retained2 = "2016-01-07-06";

      // Create partitions in table being purged
      Partition pDeleted1 = this.hiveMetastoreTestUtils.addTestPartition(purgedTbl, ImmutableList.of(deleted1), (int) System.currentTimeMillis());
      Partition pDeleted2 = this.hiveMetastoreTestUtils.addTestPartition(purgedTbl, ImmutableList.of(deleted2), (int) System.currentTimeMillis());
      Partition pRetained1 = this.hiveMetastoreTestUtils.addTestPartition(purgedTbl, ImmutableList.of(retained1), (int) System.currentTimeMillis());
      Partition pRetained2 = this.hiveMetastoreTestUtils.addTestPartition(purgedTbl, ImmutableList.of(retained2), (int) System.currentTimeMillis());

      this.fs.mkdirs(new Path(pDeleted1.getSd().getLocation()));
      this.fs.mkdirs(new Path(pDeleted2.getSd().getLocation()));
      this.fs.mkdirs(new Path(pRetained1.getSd().getLocation()));
      this.fs.mkdirs(new Path(pRetained2.getSd().getLocation()));

      // Create partitions in table that is replacement source
      Partition rReplaced1 = this.hiveMetastoreTestUtils.addTestPartition(replacementTbl, ImmutableList.of(deleted1), (int) System.currentTimeMillis());
      Partition rReplaced2 = this.hiveMetastoreTestUtils.addTestPartition(replacementTbl, ImmutableList.of(deleted2), (int) System.currentTimeMillis());
      Partition rUntouched1 = this.hiveMetastoreTestUtils.addTestPartition(replacementTbl, ImmutableList.of(retained1), (int) System.currentTimeMillis());
      Partition rUntouched2 = this.hiveMetastoreTestUtils.addTestPartition(replacementTbl, ImmutableList.of(retained2), (int) System.currentTimeMillis());

      this.fs.mkdirs(new Path(rReplaced1.getSd().getLocation()));
      this.fs.mkdirs(new Path(rReplaced2.getSd().getLocation()));
      this.fs.mkdirs(new Path(rUntouched1.getSd().getLocation()));
      this.fs.mkdirs(new Path(rUntouched2.getSd().getLocation()));

      List<Partition> pPartitions = this.hiveMetastoreTestUtils.getLocalMetastoreClient().listPartitions(purgedDbName, purgedTableName, (short) 10);
      Assert.assertEquals(pPartitions.size(), 4);

      List<Partition> rPartitions = this.hiveMetastoreTestUtils.getLocalMetastoreClient().listPartitions(replacementDbName, replacementTableName, (short) 10);
      Assert.assertEquals(rPartitions.size(), 4);

      // Run retention
      RetentionTestHelper.clean(fs, PathUtils.combinePaths(RetentionIntegrationTest.TEST_PACKAGE_RESOURCE_NAME, "testHiveTimeBasedRetention", configFileName),
          Optional.of(PathUtils.combinePaths(RetentionIntegrationTest.TEST_PACKAGE_RESOURCE_NAME, "testHiveTimeBasedRetention", "jobProps.properties")),
          testTempPath);

      pPartitions = this.hiveMetastoreTestUtils.getLocalMetastoreClient().listPartitions(purgedDbName, purgedTableName, (short) 10);
      String[] expectedRetainedPartitions;
      if (isReplacementTest) {
        // If replacement test, 2 partitions must be replaced - hence total count must be 4
        Assert.assertEquals(pPartitions.size(), 4);
        expectedRetainedPartitions = new String[] {
            getQlPartition(purgedTbl, pRetained1).getName(),
            getQlPartition(purgedTbl, pRetained2).getName(),
            getQlPartition(purgedTbl, pDeleted1).getName(),
            getQlPartition(purgedTbl, pDeleted2).getName()};
      } else {
        // If not a replacement test, 2 partitions must be purged
        Assert.assertEquals(pPartitions.size(), 2);
        expectedRetainedPartitions = new String[] {
            getQlPartition(purgedTbl, pRetained1).getName(),
            getQlPartition(purgedTbl, pRetained2).getName()};
      }

      // Check if all available partitions are that which are expected
      assertThat(FluentIterable.from(pPartitions).transform(new Function<Partition, String>() {
        @Override
        public String apply(Partition input) {
          return getQlPartition(purgedTbl, input).getName();
        }
      }).toList(), containsInAnyOrder(expectedRetainedPartitions));

      // Check that replaced partitions are pointing to correct physical location
      if (isReplacementTest) {
        for (Partition partition : pPartitions) {
          if (getQlPartition(purgedTbl, partition).getName().equalsIgnoreCase(getQlPartition(purgedTbl, pDeleted1).getName())) {
            Assert.assertEquals(partition.getSd().getLocation(), rReplaced1.getSd().getLocation(),
                "Replaced partition location not updated.");
          }
          if (getQlPartition(purgedTbl, partition).getName().equalsIgnoreCase(getQlPartition(purgedTbl, pDeleted2).getName())) {
            Assert.assertEquals(partition.getSd().getLocation(), rReplaced2.getSd().getLocation(),
                "Replaced partition location not updated.");
          }
        }
      }

      // Irrespective of whether it is a replacement test, purged partition directories must be deleted
      Assert.assertTrue(this.fs.exists(new Path(pRetained1.getSd().getLocation())));
      Assert.assertTrue(this.fs.exists(new Path(pRetained2.getSd().getLocation())));
      Assert.assertFalse(this.fs.exists(new Path(pDeleted1.getSd().getLocation())));
      Assert.assertFalse(this.fs.exists(new Path(pDeleted2.getSd().getLocation())));

      // Replacement source partition directories must be left untouched
      Assert.assertTrue(this.fs.exists(new Path(rReplaced1.getSd().getLocation())));
      Assert.assertTrue(this.fs.exists(new Path(rReplaced2.getSd().getLocation())));
      Assert.assertTrue(this.fs.exists(new Path(rUntouched1.getSd().getLocation())));
      Assert.assertTrue(this.fs.exists(new Path(rUntouched2.getSd().getLocation())));

    } finally {
      DateTimeUtils.setCurrentMillisSystem();
    }

  }

  @Test(dependsOnMethods = { "testTimeBasedHiveRetentionWithConfigStore" })
  public void testTimeBasedHivePartitionReplacementWithConfigStore() throws Exception {
    String dbName = "hiveTestDbConfigStore";
    String tableName = "testTable";
    this.testTimeBasedHiveRetention(dbName, tableName, "replacement.conf", true);
  }

  @Test(dependsOnMethods = { "testTimeBasedHiveRetentionWithJobProps" })
  public void testTimeBasedHiveRetentionWithConfigStore() throws Exception {
    String dbName = "hiveTestDbConfigStore";
    String tableName = "testTable";
    this.testTimeBasedHiveRetention(dbName, tableName, "selection.conf", false);
  }

  @Test
  public void testTimeBasedHiveRetentionWithJobProps() throws Exception {
    String dbName = "hiveTestDb";
    String tableName = "testTable";
    this.testTimeBasedHiveRetention(dbName, tableName, "hive-retention.job", false);
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
