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
package org.apache.gobblin.data.management.copy;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDateTime;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class UnixTimestampRecursiveCopyableDatasetTest {

  String rootPath = "/tmp/src";
  String databaseName = "dbName";
  String tableName = "tableName";
  String sourceDir = rootPath + "/" + databaseName + "/" + tableName;
  private Path baseSrcDir;
  private FileSystem fs;
  private Path baseDstDir;

  private static final String NUM_LOOKBACK_DAYS_STR = "2d";
  private static final Integer MAX_NUM_DAILY_DIRS = 4;
  private static final Integer NUM_DIRS_PER_DAY = 5;
  private static final Integer NUM_FILES_PER_DIR = 3;

  @BeforeClass
  public void setUp()
      throws IOException {

    this.fs = FileSystem.getLocal(new Configuration());

    baseSrcDir = new Path(sourceDir);
    if (fs.exists(baseSrcDir)) {
      fs.delete(baseSrcDir, true);
    }
    fs.mkdirs(baseSrcDir);

    baseDstDir = new Path("/tmp/dst/dataset1/");
    if (fs.exists(baseDstDir)) {
      fs.delete(baseDstDir, true);
    }
    fs.mkdirs(baseDstDir);
  }

  @Test
  public void testGetFilesAtPath()
      throws IOException {
    //1570544993735-PT-499913495

    LocalDateTime endDate =
        LocalDateTime.now(DateTimeZone.forID(TimeAwareRecursiveCopyableDataset.DEFAULT_DATE_PATTERN_TIMEZONE));

    for (int i = 0; i < MAX_NUM_DAILY_DIRS; i++) {
      for (int j = 0; j < NUM_DIRS_PER_DAY; j++) {
        Path subDirPath =
            new Path(baseSrcDir, new Path(endDate.toDateTime().plusSeconds(60).getMillis() + "-PT-100000"));
        fs.mkdirs(subDirPath);
        for (int k = 0; k < NUM_FILES_PER_DIR; k++) {
          Path filePath = new Path(subDirPath, k + ".avro");
          fs.create(filePath);
        }
        endDate = endDate.minusMinutes(10);
      }
      endDate = endDate.minusDays(1);
    }

    PathFilter ACCEPT_ALL_PATH_FILTER = new PathFilter() {

      @Override
      public boolean accept(Path path) {
        return true;
      }
    };

    //
    // Test db level copy, Qualifying Regex: ".*([0-9]{13})-PT-.*/.*", dataset root = /tmp/src/databaseName
    //
    Properties properties = new Properties();
    properties.setProperty("gobblin.dataset.pattern", sourceDir);
    properties.setProperty(TimeAwareRecursiveCopyableDataset.DATE_PATTERN_TIMEZONE_KEY,
        TimeAwareRecursiveCopyableDataset.DEFAULT_DATE_PATTERN_TIMEZONE);
    properties.setProperty(TimeAwareRecursiveCopyableDataset.LOOKBACK_TIME_KEY, NUM_LOOKBACK_DAYS_STR);
    properties.setProperty(UnixTimestampRecursiveCopyableDataset.VERSION_SELECTION_POLICY, "ALL");
    properties.setProperty(UnixTimestampRecursiveCopyableDataset.TIMESTAMP_REGEEX, ".*/([0-9]{13})-PT-.*/.*");

    UnixTimestampCopyableDatasetFinder finder = new UnixTimestampCopyableDatasetFinder(fs, properties);

    // Snap shot selection policy = ALL
    String datasetRoot = rootPath + "/" + databaseName;
    UnixTimestampRecursiveCopyableDataset dataset = (UnixTimestampRecursiveCopyableDataset) finder.datasetAtPath(new Path(datasetRoot));
    List<FileStatus> fileStatusList = dataset.getFilesAtPath(fs, baseSrcDir, ACCEPT_ALL_PATH_FILTER);
    Assert.assertTrue(fileStatusList.size() == 30);

    // version selection policy = EARLIEST
    properties.setProperty(UnixTimestampRecursiveCopyableDataset.VERSION_SELECTION_POLICY, "EARLIEST");
    finder = new UnixTimestampCopyableDatasetFinder(fs, properties);
    dataset = (UnixTimestampRecursiveCopyableDataset) finder.datasetAtPath(new Path(datasetRoot));
    fileStatusList = dataset.getFilesAtPath(fs, baseSrcDir, ACCEPT_ALL_PATH_FILTER);
    Assert.assertTrue(fileStatusList.size() == 6);

    // version selection policy = LATEST
    properties.setProperty(UnixTimestampRecursiveCopyableDataset.VERSION_SELECTION_POLICY, "latest");
    finder = new UnixTimestampCopyableDatasetFinder(fs, properties);
    dataset = (UnixTimestampRecursiveCopyableDataset) finder.datasetAtPath(new Path(datasetRoot));
    fileStatusList = dataset.getFilesAtPath(fs, baseSrcDir, ACCEPT_ALL_PATH_FILTER);
    Assert.assertTrue(fileStatusList.size() == 6);


    //
    // Test table level copy, Qualifying Regex: ".*/([0-9]{13})-PT-.*/.*")\, dataset root = /tmp/src/databaseName/tableName
    //
    properties.setProperty(UnixTimestampRecursiveCopyableDataset.TIMESTAMP_REGEEX, "([0-9]{13})-PT-.*/.*");
    finder = new UnixTimestampCopyableDatasetFinder(fs, properties);
    datasetRoot = rootPath + "/" + databaseName + "/" + tableName;

    // Snap shot selection policy = ALL
    properties.setProperty(UnixTimestampRecursiveCopyableDataset.VERSION_SELECTION_POLICY, "ALL");
    dataset = (UnixTimestampRecursiveCopyableDataset) finder.datasetAtPath(new Path(datasetRoot));
    fileStatusList = dataset.getFilesAtPath(fs, baseSrcDir, ACCEPT_ALL_PATH_FILTER);
    Assert.assertTrue(fileStatusList.size() == 30);

    // Snap shot selection policy = EARLIEST
    properties.setProperty(UnixTimestampRecursiveCopyableDataset.VERSION_SELECTION_POLICY, "EARLIEST");
    finder = new UnixTimestampCopyableDatasetFinder(fs, properties);
    dataset = (UnixTimestampRecursiveCopyableDataset) finder.datasetAtPath(new Path(datasetRoot));
    fileStatusList = dataset.getFilesAtPath(fs, baseSrcDir, ACCEPT_ALL_PATH_FILTER);
    Assert.assertTrue(fileStatusList.size() == 6);

    // Snap shot selection policy = LATEST
    properties.setProperty(UnixTimestampRecursiveCopyableDataset.VERSION_SELECTION_POLICY, "latest");
    finder = new UnixTimestampCopyableDatasetFinder(fs, properties);
    dataset = (UnixTimestampRecursiveCopyableDataset) finder.datasetAtPath(new Path(datasetRoot));
    fileStatusList = dataset.getFilesAtPath(fs, baseSrcDir, ACCEPT_ALL_PATH_FILTER);
    Assert.assertTrue(fileStatusList.size() == 6);

  }

  @Test
  public void testRegex() {
    String dbRegex = ".*/([0-9]{13}).*/.*";
    long now = System.currentTimeMillis();
    String path = "tableName/" + now + "-PT-12345/part1.avro";
    Pattern pattern = Pattern.compile(dbRegex);
    Matcher matcher = pattern.matcher(path);
    Assert.assertTrue(matcher.matches());
    Assert.assertEquals(Long.parseLong(matcher.group(1)), now);

    String tableRegex = "([0-9]{13}).*/.*";
    path = now + "-PT-12345/part1.avro";
    pattern = Pattern.compile(tableRegex);
    matcher = pattern.matcher(path);
    Assert.assertTrue(matcher.matches());
    Assert.assertEquals(Long.parseLong(matcher.group(1)), now);
  }

  @AfterClass
  public void clean()
      throws IOException {
    //Delete tmp directories
    this.fs.delete(baseSrcDir, true);
    this.fs.delete(baseDstDir, true);
  }
}
