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

import java.io.IOException;

import org.apache.gobblin.data.management.version.TimestampedHiveDatasetVersion;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.typesafe.config.ConfigFactory;


@Test(groups = {"gobblin.data.management.version"})
public class HdfsModifiedTimeHiveVersionFinderTest {
  public static final String PARTITION_NAME = "RandomDb@RandomTable@RandomPartition";
  public static final String TIMESTAMP = "123456";
  private FileSystem fs = Mockito.mock(FileSystem.class);
  private Partition partition = Mockito.mock(Partition.class);
  private FileStatus fileStatus = Mockito.mock(FileStatus.class);
  private HdfsModifiedTimeHiveVersionFinder hdfsModifiedTimeHiveVersionFinder =
      new HdfsModifiedTimeHiveVersionFinder(this.fs, ConfigFactory.empty());

  @BeforeMethod
  public void initialize() {
    Mockito.doReturn(PARTITION_NAME).when(this.partition).getCompleteName();
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testNullPartition() {
    Partition partition = null;
    TimestampedHiveDatasetVersion datasetVersion = this.hdfsModifiedTimeHiveVersionFinder.getDatasetVersion(partition);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testNullDataLocation() {
    Mockito.doReturn(null).when(this.partition).getDataLocation();
    TimestampedHiveDatasetVersion datasetVersion =
        this.hdfsModifiedTimeHiveVersionFinder.getDatasetVersion(this.partition);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testInvalidDataLocation()
      throws IOException {
    Mockito.doReturn(new Path("Invalid Location")).when(this.partition).getDataLocation();
    Mockito.doReturn(false).when(this.fs).exists(Mockito.any(Path.class));
    TimestampedHiveDatasetVersion datasetVersion =
        this.hdfsModifiedTimeHiveVersionFinder.getDatasetVersion(this.partition);
  }

  @Test
  public void testTimeStampForVersion()
      throws IOException {
    Mockito.doReturn(new Path("Invalid Location")).when(this.partition).getDataLocation();
    Mockito.doReturn(true).when(this.fs).exists(Mockito.any(Path.class));
    Mockito.doReturn(this.fileStatus).when(this.fs).getFileStatus(Mockito.any(Path.class));
    Mockito.doReturn(Long.valueOf(TIMESTAMP)).when(this.fileStatus).getModificationTime();
    TimestampedHiveDatasetVersion datasetVersion =
        this.hdfsModifiedTimeHiveVersionFinder.getDatasetVersion(this.partition);

    // Check if the datasetVersion contains the correct partition
    Assert.assertTrue(datasetVersion.getPartition().getCompleteName().equalsIgnoreCase(PARTITION_NAME));
    // Check if the datasetVersion contains the correct modified timestamp of the underlying data location
    Assert.assertTrue(datasetVersion.getDateTime().getMillis() == Long.valueOf(TIMESTAMP));
    System.out.println(datasetVersion);
  }
}
