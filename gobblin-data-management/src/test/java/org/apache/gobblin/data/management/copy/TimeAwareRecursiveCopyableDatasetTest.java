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
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.util.filters.HiddenFilter;

@Slf4j
public class TimeAwareRecursiveCopyableDatasetTest {
  private FileSystem fs;
  private Path baseDir1;
  private Path baseDir2;

  @BeforeClass
  public void setUp() throws IOException {
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
  }

  @Test
  public void testGetFilesAtPath() throws IOException {
    String datePattern = "yyyy/MM/dd/HH";
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern(datePattern);

    LocalDateTime endDate = LocalDateTime.now();

    for (int i = 0; i < 24; i++) {
      String startDate = endDate.minusHours(i).format(formatter);
      Path subDirPath = new Path(baseDir1, new Path(startDate));
      fs.mkdirs(subDirPath);
      fs.create(new Path(subDirPath, i + ".avro"));
    }

    Properties properties = new Properties();
    properties.setProperty(TimeAwareRecursiveCopyableDataset.LOOKBACK_TIME_KEY, "4h");
    properties.setProperty(TimeAwareRecursiveCopyableDataset.DATE_PATTERN_KEY, "yyyy/MM/dd/HH");

    PathFilter pathFilter = new HiddenFilter();
    TimeAwareRecursiveCopyableDataset dataset = new TimeAwareRecursiveCopyableDataset(fs, baseDir1, properties,
        new Path("/tmp/src/*/hourly"));
    List<FileStatus> fileStatusList = dataset.getFilesAtPath(fs, baseDir1, pathFilter);

    Assert.assertEquals(fileStatusList.size(), 5);

    datePattern = "yyyy/MM/dd";
    formatter = DateTimeFormatter.ofPattern(datePattern);
    endDate = LocalDateTime.now();

    for (int i = 0; i < 3; i++) {
      String startDate = endDate.minusDays(i).format(formatter);
      Path subDirPath = new Path(baseDir2, new Path(startDate));
      fs.mkdirs(subDirPath);
      fs.create(new Path(subDirPath, i + ".avro"));
    }

    properties = new Properties();
    properties.setProperty(TimeAwareRecursiveCopyableDataset.LOOKBACK_TIME_KEY, "2d");
    properties.setProperty(TimeAwareRecursiveCopyableDataset.DATE_PATTERN_KEY, "yyyy/MM/dd");

    dataset = new TimeAwareRecursiveCopyableDataset(fs, baseDir2, properties,
        new Path("/tmp/src/*/daily"));
    fileStatusList = dataset.getFilesAtPath(fs, baseDir2, pathFilter);

    Assert.assertEquals(fileStatusList.size(), 3);
  }

  @AfterClass
  public void clean() throws IOException {
    //Delete tmp directories
    this.fs.delete(baseDir1, true);
    this.fs.delete(baseDir2, true);
  }
}