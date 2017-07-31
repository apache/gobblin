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

package org.apache.gobblin.data.management.copy.hive;

import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;


public class HiveTargetPathHelperTest {

  private static Path TABLE_ROOT = new Path("/table/path");

  private FileSystem fs;

  @BeforeMethod
  public void setUp()
      throws Exception {
    this.fs = Mockito.mock(FileSystem.class);
    Mockito.when(fs.makeQualified(Mockito.any(Path.class))).thenAnswer(new Answer<Path>() {
      @Override
      public Path answer(InvocationOnMock invocation)
          throws Throwable {
        return (Path) invocation.getArguments()[0];
      }
    });
  }

  @Test
  public void testRelocateFilesUnpartitioned() {
    Properties properties = new Properties();
    properties.setProperty(HiveTargetPathHelper.RELOCATE_DATA_FILES_KEY, Boolean.toString(true));
    properties.setProperty(HiveTargetPathHelper.COPY_TARGET_TABLE_ROOT, "/target");

    HiveTargetPathHelper helper = createTestTargetPathHelper(properties);

    Path source = new Path(TABLE_ROOT, "partition/file1");
    Assert.assertEquals(helper.getTargetPath(source, this.fs, Optional.<Partition>absent(), true), new Path("/target/tableName/file1"));
  }

  @Test
  public void testRelocateFilesPartitioned() {
    Properties properties = new Properties();
    properties.setProperty(HiveTargetPathHelper.RELOCATE_DATA_FILES_KEY, Boolean.toString(true));
    properties.setProperty(HiveTargetPathHelper.COPY_TARGET_TABLE_ROOT, "/target");

    HiveTargetPathHelper helper = createTestTargetPathHelper(properties);

    Path source = new Path(TABLE_ROOT, "partition/file1");

    Partition partition = Mockito.mock(Partition.class);
    Mockito.when(partition.getValues()).thenReturn(Lists.newArrayList("part", "123"));

    Assert.assertEquals(helper.getTargetPath(source, this.fs, Optional.of(partition), true), new Path("/target/tableName/part/123/file1"));
  }

  @Test
  public void testTokenReplacement() {
    Properties properties = new Properties();
    properties.setProperty(HiveTargetPathHelper.RELOCATE_DATA_FILES_KEY, Boolean.toString(true));
    properties.setProperty(HiveTargetPathHelper.COPY_TARGET_TABLE_ROOT, "/target/$DB/$TABLE");

    HiveTargetPathHelper helper = createTestTargetPathHelper(properties);

    Path source = new Path(TABLE_ROOT, "partition/file1");
    Assert.assertEquals(helper.getTargetPath(source, this.fs, Optional.<Partition>absent(), true), new Path("/target/dbName/tableName/file1"));
  }

  @Test
  public void testReplacePrefix() {
    Properties properties = new Properties();
    properties.setProperty(HiveTargetPathHelper.COPY_TARGET_TABLE_PREFIX_TOBE_REPLACED, "/table");
    properties.setProperty(HiveTargetPathHelper.COPY_TARGET_TABLE_PREFIX_REPLACEMENT, "/replaced");

    HiveTargetPathHelper helper = createTestTargetPathHelper(properties);

    Path source = new Path(TABLE_ROOT, "partition/file1");
    Assert.assertEquals(helper.getTargetPath(source, this.fs, Optional.<Partition>absent(), true), new Path("/replaced/path/partition/file1"));
  }

  @Test
  public void testNewTableRoot() {
    Properties properties = new Properties();
    properties.setProperty(HiveTargetPathHelper.COPY_TARGET_TABLE_ROOT, "/target");

    HiveTargetPathHelper helper = createTestTargetPathHelper(properties);

    Path source = new Path(TABLE_ROOT, "partition/file1");
    Assert.assertEquals(helper.getTargetPath(source, this.fs, Optional.<Partition>absent(), true), new Path("/target/tableName/partition/file1"));
  }

  @Test
  public void testReplicatePaths() {
    Properties properties = new Properties();

    HiveTargetPathHelper helper = createTestTargetPathHelper(properties);

    Path source = new Path(TABLE_ROOT, "partition/file1");
    Assert.assertEquals(helper.getTargetPath(source, this.fs, Optional.<Partition>absent(), true), new Path(TABLE_ROOT, "partition/file1"));
  }

  private HiveTargetPathHelper createTestTargetPathHelper(Properties properties) {
    HiveDataset dataset = Mockito.mock(HiveDataset.class);

    Table table = new Table(new org.apache.hadoop.hive.metastore.api.Table());
    table.setDbName("dbName");
    table.setTableName("tableName");
    Mockito.when(dataset.getTable()).thenReturn(table);

    Mockito.when(dataset.getTableRootPath()).thenReturn(Optional.of(TABLE_ROOT));
    Mockito.when(dataset.getProperties()).thenReturn(properties);

    HiveTargetPathHelper helper = new HiveTargetPathHelper(dataset);

    return helper;
  }

}
