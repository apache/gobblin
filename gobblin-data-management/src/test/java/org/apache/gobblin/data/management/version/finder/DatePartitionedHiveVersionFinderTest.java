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
package org.apache.gobblin.data.management.version.finder;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.is;

import java.net.URLDecoder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.data.management.conversion.hive.LocalHiveMetastoreTestUtils;
import org.apache.gobblin.data.management.version.TimestampedHiveDatasetVersion;


@Test(groups = { "gobblin.data.management.version" })
public class DatePartitionedHiveVersionFinderTest {

  private FileSystem fs;
  private LocalHiveMetastoreTestUtils hiveMetastoreTestUtils;
  private DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy/MM/dd/HH").withZone(DateTimeZone.forID(ConfigurationKeys.PST_TIMEZONE_NAME));
  private String dbName = "VfDb1";

  @BeforeClass
  public void setup() throws Exception {
    this.fs = FileSystem.getLocal(new Configuration());
    this.hiveMetastoreTestUtils = LocalHiveMetastoreTestUtils.getInstance();
    this.hiveMetastoreTestUtils.dropDatabaseIfExists(this.dbName);
    this.hiveMetastoreTestUtils.createTestDb(this.dbName);
  }

  @AfterClass
  public void cleanUp() {
    try {
      this.hiveMetastoreTestUtils.dropDatabaseIfExists(this.dbName);
    } catch (Exception e) {
      // Will get cleaned up next run of test
    }
  }

  @Test
  public void testDefaults() throws Exception {

    DatePartitionHiveVersionFinder versionFinder = new DatePartitionHiveVersionFinder(this.fs, ConfigFactory.empty());
    String tableName = "VfTb1";

    Table tbl = this.hiveMetastoreTestUtils.createTestAvroTable(dbName, tableName, ImmutableList.of("datepartition"));
    org.apache.hadoop.hive.metastore.api.Partition tp =
        this.hiveMetastoreTestUtils.addTestPartition(tbl, ImmutableList.of("2016-01-01-20"), (int) System.currentTimeMillis());
    Partition partition = new Partition(new org.apache.hadoop.hive.ql.metadata.Table(tbl), tp);

    assertThat(partition.getName(), is("datepartition=2016-01-01-20"));
    TimestampedHiveDatasetVersion dv = versionFinder.getDatasetVersion(partition);
    Assert.assertEquals(dv.getDateTime(), formatter.parseDateTime("2016/01/01/20"));

  }

  @Test
  public void testUserDefinedDatePattern() throws Exception {
    String tableName = "VfTb2";
    Config conf =
        ConfigFactory.parseMap(ImmutableMap.<String, String> of(DatePartitionHiveVersionFinder.PARTITION_KEY_NAME_KEY, "field1",
            DatePartitionHiveVersionFinder.PARTITION_VALUE_DATE_TIME_PATTERN_KEY, "yyyy/MM/dd/HH"));

    DatePartitionHiveVersionFinder versionFinder = new DatePartitionHiveVersionFinder(this.fs, conf);

    Table tbl = this.hiveMetastoreTestUtils.createTestAvroTable(dbName, tableName, ImmutableList.of("field1"));
    org.apache.hadoop.hive.metastore.api.Partition tp =
        this.hiveMetastoreTestUtils.addTestPartition(tbl, ImmutableList.of("2016/01/01/20"), (int) System.currentTimeMillis());
    Partition partition = new Partition(new org.apache.hadoop.hive.ql.metadata.Table(tbl), tp);
    Assert.assertEquals(URLDecoder.decode(partition.getName(), "UTF-8"), "field1=2016/01/01/20");
    TimestampedHiveDatasetVersion dv = versionFinder.getDatasetVersion(partition);
    Assert.assertEquals(dv.getDateTime(), formatter.parseDateTime("2016/01/01/20"));
  }

  @Test
  public void testMultiplePartitionFields() throws Exception {
    DatePartitionHiveVersionFinder versionFinder = new DatePartitionHiveVersionFinder(this.fs, ConfigFactory.empty());
    String tableName = "VfTb3";

    Table tbl = this.hiveMetastoreTestUtils.createTestAvroTable(dbName, tableName, ImmutableList.of("datepartition", "field1"));
    org.apache.hadoop.hive.metastore.api.Partition tp =
        this.hiveMetastoreTestUtils.addTestPartition(tbl, ImmutableList.of("2016-01-01-20", "f1"), (int) System.currentTimeMillis());
    Partition partition = new Partition(new org.apache.hadoop.hive.ql.metadata.Table(tbl), tp);

    assertThat(partition.getName(), anyOf(is("field1=f1/datepartition=2016-01-01-20"), is("datepartition=2016-01-01-20/field1=f1")));
    TimestampedHiveDatasetVersion dv = versionFinder.getDatasetVersion(partition);
    Assert.assertEquals(dv.getDateTime(), formatter.parseDateTime("2016/01/01/20"));

  }
}
