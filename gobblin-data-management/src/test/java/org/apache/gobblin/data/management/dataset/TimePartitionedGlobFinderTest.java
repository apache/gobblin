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
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import org.apache.gobblin.dataset.FileSystemDataset;


public class TimePartitionedGlobFinderTest {

  private Path testRootDir;
  private FileSystem localFs;
  private ZoneId zone;
  private ZonedDateTime curTime;

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
    zone = ZoneId.of("America/Los_Angeles");
    LocalDateTime localTime = LocalDateTime.of(2019,1,1,0,0);
    curTime = ZonedDateTime.of(localTime, zone);
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
    createPartitions(ds1, hourlyPrefix,0, 2, hourlyFormat);
    createPartitions(ds2, hourlyPrefix,-1, 2, hourlyFormat);

    String datasetPattern = new Path(testRootDir, "*/*").toString();

    // Test glob finder without creating empty partition
    Properties props = new Properties();
    props.setProperty("gobblin.dataset.pattern", datasetPattern);
    props.setProperty("timePartitionGlobFinder.partitionPrefix", hourlyPrefix);
    props.setProperty("timePartitionGlobFinder.timeFormat", dayFormat);
    props.setProperty("timePartitionGlobFinder.lookbackSpec", "P2D");
    props.setProperty("timePartitionGlobFinder.granularity", "DAY");
    TimePartitionGlobFinder finder = new TimePartitionGlobFinder(localFs, props, curTime);
    List<FileSystemDataset> datasets = finder.findDatasets();
    Assert.assertEquals(datasets.size(), 2);
    // Verify there are 2 day partitions for /db2/table2
    Assert.assertNotNull(find(getPartitionPath(ds1, hourlyPrefix, 0, dayFormat), datasets));
    Assert.assertNotNull(find(getPartitionPath(ds2, hourlyPrefix, -1, dayFormat), datasets));

    // Test glob finder with creating empty partition
    props.setProperty("timePartitionGlobFinder.enableVirtualPartition", "true");
    finder = new TimePartitionGlobFinder(localFs, props, curTime);
    datasets = finder.findDatasets();
    Assert.assertEquals(datasets.size(), 6);
    // Verify virtual partitions for /db1/table1
    Assert.assertTrue(find(getPartitionPath(ds1, hourlyPrefix, -1, dayFormat), datasets).isVirtual());
    Assert.assertTrue(find(getPartitionPath(ds1, hourlyPrefix, -2, dayFormat), datasets).isVirtual());
    // Verify virtual partitions for /db2/table2
    Assert.assertTrue(find(getPartitionPath(ds2, hourlyPrefix, 0, dayFormat), datasets).isVirtual());
    Assert.assertTrue(find(getPartitionPath(ds2, hourlyPrefix, -2, dayFormat), datasets).isVirtual());
  }

  private Path getPartitionPath(Path dataset, String prefix, int dayOffset, String format) {
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format);
    return new Path(dataset, prefix + formatter.format(curTime.plusDays(dayOffset)));
  }

  private SimpleFileSystemDataset find(Path path, List<FileSystemDataset> list) {
    for (FileSystemDataset dataset : list) {
      if (dataset.datasetRoot().equals(path)) {
        return (SimpleFileSystemDataset)dataset;
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
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format);
    ZonedDateTime dayTime = curTime.plusDays(dayOffset);
    for (int i = 0; i < hours; i++) {
      Path hourPath = new Path(formatter.format(dayTime.withHour(i)));
      Path datasetPartitionPath = new Path(dataset, prefix + hourPath);
      Path dataFile = new Path(datasetPartitionPath, "dataFile");
      try (OutputStream outputStream = localFs.create(dataFile, true)) {
        outputStream.write(i);
      }
    }
  }

  @Test
  public void testDerivePartitionPattern() {
    String slashTimeFormat = "yyyy/MM/dd";
    String dashTimeFormat = "yyyy-MM-dd";

    // 2019/12/1 - 2019/12/3
    LocalDateTime localTime = LocalDateTime.of(2019,12,3,0,0);
    ZonedDateTime end = ZonedDateTime.of(localTime, zone);
    ZonedDateTime start = end.withDayOfMonth(1);
    Assert.assertEquals(TimePartitionGlobFinder.derivePartitionPattern(start, end, slashTimeFormat),
        "{2019}/{12}/*");
    Assert.assertEquals(TimePartitionGlobFinder.derivePartitionPattern(start, end, dashTimeFormat),
        "{2019}-{12}*");

    // 2019/11/30 - 2019/12/3
    start = end.withMonth(11).withDayOfMonth(30);
    Assert.assertEquals(TimePartitionGlobFinder.derivePartitionPattern(start, end, slashTimeFormat),
        "{2019}/{11,12}/*");
    Assert.assertEquals(TimePartitionGlobFinder.derivePartitionPattern(start, end, dashTimeFormat),
        "{2019}-{11,12}*");

    // 2018/12/1 - 2019/12/3
    start = end.withYear(2018).withMonth(12).withDayOfMonth(1);
    Assert.assertEquals(TimePartitionGlobFinder.derivePartitionPattern(start, end, slashTimeFormat),
        "{2018,2019}/*/*");
    Assert.assertEquals(TimePartitionGlobFinder.derivePartitionPattern(start, end, dashTimeFormat),
        "{2018,2019}-*");

    // 2018/11/30 - 2019/12/3
    start = end.withYear(2018).withMonth(11).withDayOfMonth(30);
    Assert.assertEquals(TimePartitionGlobFinder.derivePartitionPattern(start, end, slashTimeFormat),
        "{2018,2019}/*/*");
    Assert.assertEquals(TimePartitionGlobFinder.derivePartitionPattern(start, end, dashTimeFormat),
        "{2018,2019}-*");

    // 2018/11/30 - 2019/01/3
    end = end.withMonth(1);
    Assert.assertEquals(TimePartitionGlobFinder.derivePartitionPattern(start, end, slashTimeFormat),
        "{2018,2019}/{11,12,01}/*");
    Assert.assertEquals(TimePartitionGlobFinder.derivePartitionPattern(start, end, dashTimeFormat),
        "{2018,2019}-{11,12,01}*");

    // Test hourly
    Assert.assertEquals(TimePartitionGlobFinder.derivePartitionPattern(start, end, "yyyy/MM/dd/HH"),
        "{2018,2019}/{11,12,01}/*/*");
    Assert.assertEquals(TimePartitionGlobFinder.derivePartitionPattern(start, end, "yyyy-MM-dd-HH"),
        "{2018,2019}-{11,12,01}*");

    // 2019/1/1 - 2019/1/3
    start = start.withYear(2019).withMonth(1).withDayOfMonth(1);
    Assert.assertEquals(TimePartitionGlobFinder.derivePartitionPattern(start, end, slashTimeFormat),
        "{2019}/{01}/*");
    Assert.assertEquals(TimePartitionGlobFinder.derivePartitionPattern(start, end, dashTimeFormat),
        "{2019}-{01}*");
  }

  @Test
  public void testSupportedTimeFormat() {
    Assert.assertTrue(TimePartitionGlobFinder.isTimeFormatSupported("yyyy/MM/dd/HH"));
    Assert.assertTrue(TimePartitionGlobFinder.isTimeFormatSupported("yyyy/MM/dd"));
    Assert.assertFalse(TimePartitionGlobFinder.isTimeFormatSupported("MM/dd/yyyy"));
    Assert.assertTrue(TimePartitionGlobFinder.isTimeFormatSupported("yyyy-MM-dd"));
    Assert.assertTrue(TimePartitionGlobFinder.isTimeFormatSupported("yyyy-MM-dd-HH"));
    Assert.assertFalse(TimePartitionGlobFinder.isTimeFormatSupported("MM-dd-yyyy"));
  }

  @AfterClass
  private void cleanup()
      throws IOException {
    if (localFs != null && localFs.exists(testRootDir)) {
      localFs.delete(testRootDir, true);
    }
  }
}
