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

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDateTime;
import org.joda.time.Period;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import org.apache.gobblin.util.PathUtils;
import org.apache.gobblin.util.filters.HiddenFilter;

@Slf4j
public class TimeAwareRecursiveCopyableDatasetTest {
  private FileSystem fs;
  private Path baseDir1;
  private Path baseDir2;
  private Path baseDir3;
  private Path baseDir4;

  private static final String NUM_LOOKBACK_DAYS_STR = "2d";
  private static final Integer NUM_LOOKBACK_DAYS = 2;
  private static final String NUM_LOOKBACK_HOURS_STR = "4h";
  private static final Integer NUM_LOOKBACK_HOURS = 4;
  private static final Integer MAX_NUM_DAILY_DIRS = 4;
  private static final Integer MAX_NUM_HOURLY_DIRS = 48;
  private static final String NUM_LOOKBACK_DAYS_HOURS_STR = "1d1h";
  private static final Integer NUM_DAYS_HOURS_DIRS = 25;
  private static final String NUM_LOOKBACK_HOURS_MINS_STR = "1h1m";

  @BeforeClass
  public void setUp() throws IOException {
    Assert.assertTrue(NUM_LOOKBACK_DAYS < MAX_NUM_DAILY_DIRS);
    Assert.assertTrue(NUM_LOOKBACK_HOURS < MAX_NUM_HOURLY_DIRS);

    this.fs = FileSystem.getLocal(new Configuration());

    baseDir1 = new Path("/tmp/src/ds1/hourly");
    if (fs.exists(baseDir1)) {
      fs.delete(baseDir1, true);
    }
    fs.mkdirs(baseDir1);

    baseDir2 = new Path("/tmp/src/ds1/daily");
    if (fs.exists(baseDir2)) {
      fs.delete(baseDir2, true);
    }
    fs.mkdirs(baseDir2);

    baseDir3 = new Path("/tmp/src/ds2/daily");
    if (fs.exists(baseDir3)) {
      fs.delete(baseDir3, true);
    }
    fs.mkdirs(baseDir3);

    baseDir4 = new Path("/tmp/src/ds3/daily");
    if (fs.exists(baseDir4)) {
      fs.delete(baseDir4, true);
    }
    fs.mkdirs(baseDir4);
    PeriodFormatter formatter = new PeriodFormatterBuilder().appendDays().appendSuffix("d").appendHours().appendSuffix("h").toFormatter();
    Period period = formatter.parsePeriod(NUM_LOOKBACK_DAYS_HOURS_STR);
  }

  @Test
  public void testGetFilesAtPath() throws IOException {
    String datePattern = "yyyy/MM/dd/HH";
    DateTimeFormatter formatter = DateTimeFormat.forPattern(datePattern);

    LocalDateTime endDate = LocalDateTime.now(DateTimeZone.forID(TimeAwareRecursiveCopyableDataset.DEFAULT_DATE_PATTERN_TIMEZONE));

    Set<String> candidateFiles = new HashSet<>();
    for (int i = 0; i < MAX_NUM_HOURLY_DIRS; i++) {
      String startDate = endDate.minusHours(i).toString(formatter);
      Path subDirPath = new Path(baseDir1, new Path(startDate));
      fs.mkdirs(subDirPath);
      Path filePath = new Path(subDirPath, i + ".avro");
      fs.create(filePath);
      if (i < (NUM_LOOKBACK_HOURS + 1)) {
        candidateFiles.add(filePath.toString());
      }
    }

    //Lookback time = "4h"
    Properties properties = new Properties();
    properties.setProperty(TimeAwareRecursiveCopyableDataset.LOOKBACK_TIME_KEY, NUM_LOOKBACK_HOURS_STR);
    properties.setProperty(TimeAwareRecursiveCopyableDataset.DATE_PATTERN_KEY, "yyyy/MM/dd/HH");

    PathFilter pathFilter = new HiddenFilter();
    TimeAwareRecursiveCopyableDataset dataset = new TimeAwareRecursiveCopyableDataset(fs, baseDir1, properties,
        new Path("/tmp/src/*/hourly"));
    List<FileStatus> fileStatusList = dataset.getFilesAtPath(fs, baseDir1, pathFilter);

    Assert.assertEquals(fileStatusList.size(), NUM_LOOKBACK_HOURS + 1);

    for (FileStatus fileStatus: fileStatusList) {
      Assert.assertTrue(candidateFiles.contains(PathUtils.getPathWithoutSchemeAndAuthority(fileStatus.getPath()).toString()));
    }

    //Lookback time = "1d1h"
    properties = new Properties();
    properties.setProperty(TimeAwareRecursiveCopyableDataset.LOOKBACK_TIME_KEY, NUM_LOOKBACK_DAYS_HOURS_STR);
    properties.setProperty(TimeAwareRecursiveCopyableDataset.DATE_PATTERN_KEY, "yyyy/MM/dd/HH");
    dataset = new TimeAwareRecursiveCopyableDataset(fs, baseDir1, properties,
        new Path("/tmp/src/*/hourly"));
    fileStatusList = dataset.getFilesAtPath(fs, baseDir1, pathFilter);
    candidateFiles = new HashSet<>();
    datePattern = "yyyy/MM/dd/HH";
    formatter = DateTimeFormat.forPattern(datePattern);

    for (int i = 0; i < MAX_NUM_HOURLY_DIRS; i++) {
      String startDate = endDate.minusHours(i).toString(formatter);
      Path subDirPath = new Path(baseDir1, new Path(startDate));
      Path filePath = new Path(subDirPath, i + ".avro");
      if (i < NUM_DAYS_HOURS_DIRS + 1) {
        candidateFiles.add(filePath.toString());
      }
    }

    Assert.assertEquals(fileStatusList.size(), NUM_DAYS_HOURS_DIRS + 1);
    for (FileStatus fileStatus: fileStatusList) {
      Assert.assertTrue(candidateFiles.contains(PathUtils.getPathWithoutSchemeAndAuthority(fileStatus.getPath()).toString()));
    }

    //Lookback time = "2d"
    datePattern = "yyyy/MM/dd";
    formatter = DateTimeFormat.forPattern(datePattern);
    endDate = LocalDateTime.now(DateTimeZone.forID(TimeAwareRecursiveCopyableDataset.DEFAULT_DATE_PATTERN_TIMEZONE));

    candidateFiles = new HashSet<>();
    for (int i = 0; i < MAX_NUM_DAILY_DIRS; i++) {
      String startDate = endDate.minusDays(i).toString(formatter);
      Path subDirPath = new Path(baseDir2, new Path(startDate));
      fs.mkdirs(subDirPath);
      Path filePath = new Path(subDirPath, i + ".avro");
      fs.create(filePath);
      if (i < (NUM_LOOKBACK_DAYS + 1)) {
        candidateFiles.add(filePath.toString());
      }
    }
    // Edge case: test that files that do not match dateformat but within the folders searched by the timeaware finder is ignored
    File f = new File(baseDir2.toString() + "/metadata.test");

    f.createNewFile();

    properties = new Properties();
    properties.setProperty(TimeAwareRecursiveCopyableDataset.LOOKBACK_TIME_KEY, NUM_LOOKBACK_DAYS_STR);
    properties.setProperty(TimeAwareRecursiveCopyableDataset.DATE_PATTERN_KEY, "yyyy/MM/dd");

    dataset = new TimeAwareRecursiveCopyableDataset(fs, baseDir2, properties,
        new Path("/tmp/src/*/daily"));
    fileStatusList = dataset.getFilesAtPath(fs, baseDir2, pathFilter);

    Assert.assertEquals(fileStatusList.size(), NUM_LOOKBACK_DAYS + 1);
    for (FileStatus fileStatus: fileStatusList) {
      Assert.assertTrue(candidateFiles.contains(PathUtils.getPathWithoutSchemeAndAuthority(fileStatus.getPath()).toString()));
    }

    // test ds of daily/yyyy-MM-dd-HH-mm
    datePattern = "yyyy-MM-dd-HH-mm";
    formatter = DateTimeFormat.forPattern(datePattern);
    endDate = LocalDateTime.now(DateTimeZone.forID(TimeAwareRecursiveCopyableDataset.DEFAULT_DATE_PATTERN_TIMEZONE));

    Random random = new Random();

    candidateFiles = new HashSet<>();
    for (int i = 0; i < MAX_NUM_DAILY_DIRS; i++) {
      String startDate = endDate.minusDays(i).withMinuteOfHour(random.nextInt(60)).toString(formatter);
      if (i == 0) {
        // avoid future dates on minutes, so have consistency test result
        startDate = endDate.minusHours(i).withMinuteOfHour(0).toString(formatter);
      }
      Path subDirPath = new Path(baseDir3, new Path(startDate));
      fs.mkdirs(subDirPath);
      Path filePath = new Path(subDirPath, i + ".avro");
      fs.create(filePath);
      if (i < (NUM_LOOKBACK_DAYS + 1)) {
        candidateFiles.add(filePath.toString());
      }
    }

    properties = new Properties();
    properties.setProperty(TimeAwareRecursiveCopyableDataset.LOOKBACK_TIME_KEY, "2d1h");
    properties.setProperty(TimeAwareRecursiveCopyableDataset.DATE_PATTERN_KEY, "yyyy-MM-dd-HH-mm");

    dataset = new TimeAwareRecursiveCopyableDataset(fs, baseDir3, properties,
        new Path("/tmp/src/ds2/daily"));

    fileStatusList = dataset.getFilesAtPath(fs, baseDir3, pathFilter);

    Assert.assertEquals(fileStatusList.size(), NUM_LOOKBACK_DAYS + 1);
    for (FileStatus fileStatus: fileStatusList) {
      Assert.assertTrue(candidateFiles.contains(PathUtils.getPathWithoutSchemeAndAuthority(fileStatus.getPath()).toString()));
    }

    // test ds of daily/yyyy-MM-dd-HH-mm-ss
    datePattern = "yyyy-MM-dd-HH-mm-ss";
    formatter = DateTimeFormat.forPattern(datePattern);
    endDate = LocalDateTime.now(DateTimeZone.forID(TimeAwareRecursiveCopyableDataset.DEFAULT_DATE_PATTERN_TIMEZONE));

    candidateFiles = new HashSet<>();
    for (int i = 0; i < MAX_NUM_DAILY_DIRS; i++) {
      String startDate = endDate.minusDays(i).withMinuteOfHour(random.nextInt(60)).withSecondOfMinute(random.nextInt(60)).toString(formatter);
      if (i == 0) {
        // avoid future dates on minutes, so have consistency test result
        startDate = endDate.minusHours(i).withMinuteOfHour(0).withSecondOfMinute(0).toString(formatter);
      }
      Path subDirPath = new Path(baseDir4, new Path(startDate));
      fs.mkdirs(subDirPath);
      Path filePath = new Path(subDirPath, i + ".avro");
      fs.create(filePath);
      if (i < (NUM_LOOKBACK_DAYS + 1)) {
        candidateFiles.add(filePath.toString());
      }
    }

    properties = new Properties();
    properties.setProperty(TimeAwareRecursiveCopyableDataset.LOOKBACK_TIME_KEY, "2d1h");
    properties.setProperty(TimeAwareRecursiveCopyableDataset.DATE_PATTERN_KEY, "yyyy-MM-dd-HH-mm-ss");

    dataset = new TimeAwareRecursiveCopyableDataset(fs, baseDir4, properties,
        new Path("/tmp/src/ds3/daily"));

    fileStatusList = dataset.getFilesAtPath(fs, baseDir4, pathFilter);

    Assert.assertEquals(fileStatusList.size(), NUM_LOOKBACK_DAYS + 1);
    for (FileStatus fileStatus: fileStatusList) {
      Assert.assertTrue(candidateFiles.contains(PathUtils.getPathWithoutSchemeAndAuthority(fileStatus.getPath()).toString()));
    }
  }

  @Test
  public void testTimezoneProperty() throws IOException {
    // Test in UTC instead of default time
    String datePattern = "yyyy/MM/dd/HH";
    DateTimeFormatter formatter = DateTimeFormat.forPattern(datePattern);
    // Ensure that the files are created in UTC time
    LocalDateTime endDate = LocalDateTime.now(DateTimeZone.forID("UTC"));

    Set<String> candidateFiles = new HashSet<>();
    for (int i = 0; i < MAX_NUM_HOURLY_DIRS; i++) {
      String startDate = endDate.minusHours(i).toString(formatter);
      Path subDirPath = new Path(baseDir1, new Path(startDate));
      fs.mkdirs(subDirPath);
      Path filePath = new Path(subDirPath, i + ".avro");
      fs.create(filePath);
      if (i < (NUM_LOOKBACK_HOURS + 1)) {
        candidateFiles.add(filePath.toString());
      }
    }

    //Lookback time = "4h"
    Properties properties = new Properties();
    properties.setProperty(TimeAwareRecursiveCopyableDataset.LOOKBACK_TIME_KEY, NUM_LOOKBACK_HOURS_STR);
    properties.setProperty(TimeAwareRecursiveCopyableDataset.DATE_PATTERN_KEY, "yyyy/MM/dd/HH");
    properties.setProperty(TimeAwareRecursiveCopyableDataset.DATE_PATTERN_TIMEZONE_KEY, "UTC");

    PathFilter pathFilter = new HiddenFilter();
    TimeAwareRecursiveCopyableDataset dataset = new TimeAwareRecursiveCopyableDataset(fs, baseDir1, properties,
        new Path("/tmp/src/*/hourly"));
    List<FileStatus> fileStatusList = dataset.getFilesAtPath(fs, baseDir1, pathFilter);

    Assert.assertEquals(fileStatusList.size(), NUM_LOOKBACK_HOURS + 1);

    for (FileStatus fileStatus: fileStatusList) {
      Assert.assertTrue(candidateFiles.contains(PathUtils.getPathWithoutSchemeAndAuthority(fileStatus.getPath()).toString()));
    }
  }

  @Test (expectedExceptions = IllegalArgumentException.class)
  public void testInstantiationError() {
    //Daily directories, but look back time has days and hours. We should expect an assertion error.
    Properties properties = new Properties();
    properties.setProperty(TimeAwareRecursiveCopyableDataset.LOOKBACK_TIME_KEY, NUM_LOOKBACK_DAYS_HOURS_STR);
    properties.setProperty(TimeAwareRecursiveCopyableDataset.DATE_PATTERN_KEY, "yyyy/MM/dd");

    TimeAwareRecursiveCopyableDataset dataset = new TimeAwareRecursiveCopyableDataset(fs, baseDir2, properties,
        new Path("/tmp/src/*/daily"));

    // hourly directories, but look back time has hours and minutes. We should expect an assertion error.
    properties = new Properties();
    properties.setProperty(TimeAwareRecursiveCopyableDataset.LOOKBACK_TIME_KEY, NUM_LOOKBACK_HOURS_MINS_STR);
    properties.setProperty(TimeAwareRecursiveCopyableDataset.DATE_PATTERN_KEY, "yyyy-MM-dd-HH");

    dataset = new TimeAwareRecursiveCopyableDataset(fs, baseDir3, properties,
        new Path("/tmp/src/ds2/daily"));

  }

  @Test (expectedExceptions = IllegalArgumentException.class)
  public void testIllegalTimezoneProperty() throws IOException {
    //Lookback time = "4h"
    Properties properties = new Properties();
    properties.setProperty(TimeAwareRecursiveCopyableDataset.LOOKBACK_TIME_KEY, NUM_LOOKBACK_HOURS_STR);
    properties.setProperty(TimeAwareRecursiveCopyableDataset.DATE_PATTERN_KEY, "yyyy/MM/dd/HH");
    properties.setProperty(TimeAwareRecursiveCopyableDataset.DATE_PATTERN_TIMEZONE_KEY, "InvalidTimeZone");

    TimeAwareRecursiveCopyableDataset dataset = new TimeAwareRecursiveCopyableDataset(fs, baseDir3, properties,
        new Path("/tmp/src/ds2/daily"));
  }

  @Test
  public void testCheckPathDateTimeValidity() {
    String datePattern = "yyyy/MM/dd/HH";
    DateTimeFormatter formatter = DateTimeFormat.forPattern(datePattern);
    LocalDateTime startDate = LocalDateTime.parse("2022/11/30/23", formatter);
    LocalDateTime endDate = LocalDateTime.parse("2022/12/30/23", formatter);

    // Level 1 is when datePath is "", that case is taken care of in the recursivelyGetFilesAtDatePath function
    // Check when year granularity is not in range
    Assert.assertFalse(TimeAwareRecursiveCopyableDataset.checkPathDateTimeValidity(startDate, endDate, "2023", datePattern, 2));
    Assert.assertFalse(TimeAwareRecursiveCopyableDataset.checkPathDateTimeValidity(startDate, endDate, "2023/11", datePattern, 3));
    Assert.assertFalse(TimeAwareRecursiveCopyableDataset.checkPathDateTimeValidity(startDate, endDate, "2023/11/30", datePattern, 4));
    Assert.assertFalse(TimeAwareRecursiveCopyableDataset.checkPathDateTimeValidity(startDate, endDate, "2023/11/30/20", datePattern, 5));

    // Check when hour granularity is not in range
    Assert.assertTrue(TimeAwareRecursiveCopyableDataset.checkPathDateTimeValidity(startDate, endDate, "2022", datePattern, 2));
    Assert.assertTrue(TimeAwareRecursiveCopyableDataset.checkPathDateTimeValidity(startDate, endDate, "2022/11", datePattern, 3));
    Assert.assertTrue(TimeAwareRecursiveCopyableDataset.checkPathDateTimeValidity(startDate, endDate, "2022/11/30", datePattern, 4));
    Assert.assertFalse(TimeAwareRecursiveCopyableDataset.checkPathDateTimeValidity(startDate, endDate, "2022/11/30/20", datePattern, 5));

    // Change format and check that all granularities are in range
    datePattern = "yyyy/MM/dd/HH/mm";
    formatter = DateTimeFormat.forPattern(datePattern);
    startDate = LocalDateTime.parse("2022/11/30/23/59", formatter);
    endDate = LocalDateTime.parse("2022/12/30/23/59", formatter);
    Assert.assertTrue(TimeAwareRecursiveCopyableDataset.checkPathDateTimeValidity(startDate, endDate, "2022", datePattern, 2));
    Assert.assertTrue(TimeAwareRecursiveCopyableDataset.checkPathDateTimeValidity(startDate, endDate, "2022/12", datePattern, 3));
    Assert.assertTrue(TimeAwareRecursiveCopyableDataset.checkPathDateTimeValidity(startDate, endDate, "2022/12/15", datePattern, 4));
    Assert.assertTrue(TimeAwareRecursiveCopyableDataset.checkPathDateTimeValidity(startDate, endDate, "2022/12/15/15", datePattern, 5));
    Assert.assertTrue(TimeAwareRecursiveCopyableDataset.checkPathDateTimeValidity(startDate, endDate, "2022/12/15/15/30", datePattern, 6));

    // Check when invalid datePath provided when compared against datePattern
    Assert.assertFalse(TimeAwareRecursiveCopyableDataset.checkPathDateTimeValidity(startDate, endDate, "test", datePattern, 2));
    Assert.assertFalse(TimeAwareRecursiveCopyableDataset.checkPathDateTimeValidity(startDate, endDate, "test/test", datePattern, 3));
    Assert.assertFalse(TimeAwareRecursiveCopyableDataset.checkPathDateTimeValidity(startDate, endDate, "test/test/test", datePattern, 4));
  }

  @AfterClass
  public void clean() throws IOException {
    //Delete tmp directories
    this.fs.delete(baseDir1, true);
    this.fs.delete(baseDir2, true);
    this.fs.delete(baseDir3, true);
    this.fs.delete(baseDir4, true);
  }
}