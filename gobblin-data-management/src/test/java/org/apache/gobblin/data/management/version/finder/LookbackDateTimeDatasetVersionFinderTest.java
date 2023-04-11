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

import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.data.management.version.TimestampedDatasetVersion;
import org.apache.gobblin.dataset.Dataset;
import org.apache.gobblin.dataset.FileSystemDataset;
import org.apache.gobblin.util.ConfigUtils;


@Test(groups = { "gobblin.data.management.version" })
public class LookbackDateTimeDatasetVersionFinderTest {

  private FileSystem fs;
  private DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy/MM/dd/HH").withZone(DateTimeZone.forID(ConfigurationKeys.PST_TIMEZONE_NAME));
  private final Instant fixedTime = Instant.parse("2023-01-01T12:30:00.000-08:00");

  @Test
  public void testHourlyVersions() throws Exception {
    Properties properties = new Properties();
    properties.put(DateTimeDatasetVersionFinder.DATE_TIME_PATTERN_KEY, "yyyy/MM/dd/HH");
    properties.put(LookbackDateTimeDatasetVersionFinder.VERSION_PATH_PREFIX, "hourly");
    properties.put(LookbackDateTimeDatasetVersionFinder.VERSION_LOOKBACK_PERIOD, "96h");

    LookbackDateTimeDatasetVersionFinder versionFinder = new LookbackDateTimeDatasetVersionFinder(FileSystem.getLocal(new Configuration()),
        ConfigUtils.propertiesToConfig(properties), fixedTime);
    Dataset dataset = new TestDataset(new Path("/data/Dataset1"));
    Collection<TimestampedDatasetVersion> datasetVersions = versionFinder.findDatasetVersions(dataset);
    List<TimestampedDatasetVersion> sortedVersions = datasetVersions.stream().sorted().collect(Collectors.toList());
    Assert.assertEquals(datasetVersions.size(), 97);
    Assert.assertEquals(sortedVersions.get(0).getVersion().toString(formatter), "2022/12/28/12");
    Assert.assertEquals(sortedVersions.get(0).getPath().toString(), "/data/Dataset1/hourly/2022/12/28/12");
    Assert.assertEquals(sortedVersions.get(sortedVersions.size() - 1).getVersion().toString(formatter), "2023/01/01/12");
    Assert.assertEquals(sortedVersions.get(sortedVersions.size() - 1).getPath().toString(), "/data/Dataset1/hourly/2023/01/01/12");
  }

  @Test
  public void testDailyVersions() throws Exception {
    Properties properties = new Properties();
    properties.put(DateTimeDatasetVersionFinder.DATE_TIME_PATTERN_KEY, "yyyy/MM/dd");
    properties.put(LookbackDateTimeDatasetVersionFinder.VERSION_PATH_PREFIX, "daily");
    properties.put(LookbackDateTimeDatasetVersionFinder.VERSION_LOOKBACK_PERIOD, "366d");

    LookbackDateTimeDatasetVersionFinder versionFinder = new LookbackDateTimeDatasetVersionFinder(FileSystem.getLocal(new Configuration()),
        ConfigUtils.propertiesToConfig(properties), fixedTime);
    Dataset dataset = new TestDataset(new Path("/data/Dataset1"));
    Collection<TimestampedDatasetVersion> datasetVersions = versionFinder.findDatasetVersions(dataset);
    List<TimestampedDatasetVersion> sortedVersions = datasetVersions.stream().sorted().collect(Collectors.toList());
    Assert.assertEquals(datasetVersions.size(), 367);
    Assert.assertEquals(sortedVersions.get(0).getVersion().toString(formatter), "2021/12/31/00");
    Assert.assertEquals(sortedVersions.get(0).getPath().toString(), "/data/Dataset1/daily/2021/12/31");
    Assert.assertEquals(sortedVersions.get(sortedVersions.size() - 1).getVersion().toString(formatter), "2023/01/01/00");
    Assert.assertEquals(sortedVersions.get(sortedVersions.size() - 1).getPath().toString(), "/data/Dataset1/daily/2023/01/01");
  }
}

class TestDataset implements FileSystemDataset {
  private final Path datasetRoot;

  public TestDataset(Path datasetRoot) {
    this.datasetRoot = datasetRoot;
  }

  public Path datasetRoot() {
    return datasetRoot;
  }

  public String datasetURN() {
    return null;
  }
}
