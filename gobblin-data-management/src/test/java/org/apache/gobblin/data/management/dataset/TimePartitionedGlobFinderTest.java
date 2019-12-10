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

package org.apache.gobblin.data.management.dataset;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import org.apache.gobblin.dataset.FileSystemDataset;


public class TimePartitionedGlobFinderTest {

  private Path testRootDir;
  private FileSystem localFs;
  private DateTime startTime;

  @BeforeClass
  private void setup()
      throws IOException {
    localFs = FileSystem.getLocal(new Configuration());
    testRootDir = new Path(Paths.get("").toAbsolutePath().toString(),
        getClass().getSimpleName());
    if (localFs.exists(testRootDir)) {
      localFs.delete(testRootDir, true);
    }
    localFs.mkdirs(testRootDir);
    localFs.deleteOnExit(testRootDir);
    startTime = DateTime.now(DateTimeZone.forID("America/Los_Angeles"));
  }

  @Test
  public void testDayPartitions()
      throws IOException {
    String hourlyFormat = "yyyy/MM/dd/HH";
    String hourlyPrefix = "hourly/";
    String dayFormat = "yyyy/MM/dd";

    // create an empty dataset /db1/table1/hourly
    Path ds1 = createDatasetPath("db1/table1");
    // create dataset /db2/table2/hourly
    Path ds2 = createDatasetPath("db2/table2");
    createPartitions(ds2, hourlyPrefix,1, 2, hourlyFormat);
    createPartitions(ds2, hourlyPrefix,2, 2, hourlyFormat);

    String datasetPattern = new Path(testRootDir, "*/*").toString();

    // Test glob finder without creating empty partition
    Properties props = new Properties();
    props.setProperty("gobblin.dataset.pattern", datasetPattern);
    props.setProperty("timePartitionGlobFinder.partitionPrefix", hourlyPrefix);
    props.setProperty("timePartitionGlobFinder.timeFormat", dayFormat);
    TimePartitionGlobFinder finder = new TimePartitionGlobFinder(localFs, props);
    List<FileSystemDataset> datasets = finder.findDatasets();
    Assert.assertEquals(datasets.size(), 2);
    // Verify there are 2 day partitions for /db2/table2
    Assert.assertNotNull(find(getPartitionPath(ds2, hourlyPrefix, 1, dayFormat), datasets));
    Assert.assertNotNull(find(getPartitionPath(ds2, hourlyPrefix, 2, dayFormat), datasets));

    // Test glob finder with creating empty partition
    props.setProperty("timePartitionGlobFinder.enableEmptyPartition", "true");
    finder = new TimePartitionGlobFinder(localFs, props);
    datasets = finder.findDatasets();
    Assert.assertEquals(datasets.size(), 3);
    // Verify yesterday partition for /db1/table1
    FileSystemDataset dt1Dataset = find(getPartitionPath(ds1, hourlyPrefix, 1, dayFormat), datasets);
    Assert.assertNotNull(dt1Dataset);
    Assert.assertTrue(dt1Dataset instanceof EmptyFileSystemDataset);
  }

  private Path getPartitionPath(Path dataset, String prefix, int dayOffset, String format) {
    DateTimeFormatter formatter = DateTimeFormat.forPattern(format);
    return new Path(dataset, prefix + formatter.print(startTime.minusDays(dayOffset)));
  }

  private FileSystemDataset find(Path path, List<FileSystemDataset> list) {
    for (FileSystemDataset dataset : list) {
      if (dataset.datasetRoot().equals(path)) {
        return dataset;
      }
    }
    return null;
  }

  private Path createDatasetPath(String dataset)
      throws IOException {
    Path datasetPath = new Path(testRootDir, dataset);
    localFs.mkdirs(datasetPath);
    return datasetPath;
  }

  private void createPartitions(Path dataset, String prefix, int dayOffset, int hours, String format)
      throws IOException {
    DateTimeFormatter formatter = DateTimeFormat.forPattern(format);
    DateTime dayTime = startTime.minusDays(dayOffset);
    for (int i = 0; i < hours; i++) {
      Path hourPath = new Path(formatter.print(dayTime.withHourOfDay(i)));
      Path datasetPartitionPath = new Path(dataset, prefix + hourPath);
      Path dataFile = new Path(datasetPartitionPath, "dataFile");
      try (OutputStream outputStream = localFs.create(dataFile, true)) {
        outputStream.write(i);
      }
    }
  }

  @AfterClass
  private void cleanup()
      throws IOException {
    if (localFs != null && localFs.exists(testRootDir)) {
      localFs.delete(testRootDir, true);
    }
  }
}
