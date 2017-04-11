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

package gobblin.data.management.retention;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.io.Files;

import gobblin.configuration.ConfigurationKeys;
import gobblin.data.management.retention.version.TimestampedDatasetVersion;
import gobblin.data.management.retention.version.finder.DateTimeDatasetVersionFinder;
import gobblin.util.PathUtils;


public class TimestampedDatasetVersionFinderTest {

  private FileSystem fs;
  private Path testDataPathDummyPath;

  @BeforeClass
  public void setup() throws Exception {
    this.fs = FileSystem.get(new Configuration());
    this.testDataPathDummyPath = new Path(Files.createTempDir().getAbsolutePath());
    this.fs.mkdirs(this.testDataPathDummyPath);
  }

  @Test
  public void testVersionParser() throws Exception {

    Properties props = new Properties();
    props.put(DateTimeDatasetVersionFinder.RETENTION_DATE_TIME_PATTERN_KEY, "yyyy/MM/dd/hh/mm");

    DateTimeDatasetVersionFinder parser = new DateTimeDatasetVersionFinder(this.fs, props);

    Assert.assertEquals(parser.versionClass(), TimestampedDatasetVersion.class);
    Assert.assertEquals(parser.globVersionPattern(), new Path("*/*/*/*/*"));
    DateTime version = parser.getDatasetVersion(new Path("2015/06/01/10/12"), this.fs.getFileStatus(testDataPathDummyPath)).getDateTime();
    Assert.assertEquals(version.getZone(), DateTimeZone.forID(ConfigurationKeys.PST_TIMEZONE_NAME));
    Assert.assertEquals(version, new DateTime(2015, 6, 1, 10, 12, 0, 0, DateTimeZone.forID(ConfigurationKeys.PST_TIMEZONE_NAME)));

    Assert.assertEquals(
        PathUtils.getPathWithoutSchemeAndAuthority(parser
            .getDatasetVersion(new Path("2015/06/01/10/12"), this.fs.getFileStatus(testDataPathDummyPath))
            .getPathsToDelete().iterator().next()),
        PathUtils.getPathWithoutSchemeAndAuthority(this.testDataPathDummyPath));

  }

  @Test
  public void testVersionParserWithTimeZone()  throws Exception {

    Properties props = new Properties();
    props.put(DateTimeDatasetVersionFinder.RETENTION_DATE_TIME_PATTERN_KEY, "yyyy/MM/dd/hh/mm");
    props.put(DateTimeDatasetVersionFinder.RETENTION_DATE_TIME_PATTERN_TIMEZONE_KEY, "UTC");

    DateTimeDatasetVersionFinder parser = new DateTimeDatasetVersionFinder(this.fs, props);

    Assert.assertEquals(parser.versionClass(), TimestampedDatasetVersion.class);
    Assert.assertEquals(parser.globVersionPattern(), new Path("*/*/*/*/*"));
    DateTime version = parser.getDatasetVersion(new Path("2015/06/01/10/12"), this.fs.getFileStatus(testDataPathDummyPath)).getDateTime();
    Assert.assertEquals(version.getZone(), DateTimeZone.forID("UTC"));
    Assert.assertEquals(version,
        new DateTime(2015, 6, 1, 10, 12, 0, 0, DateTimeZone.forID("UTC")));
    Assert.assertEquals(
        PathUtils.getPathWithoutSchemeAndAuthority(parser
            .getDatasetVersion(new Path("2015/06/01/10/12"), this.fs.getFileStatus(testDataPathDummyPath))
            .getPathsToDelete().iterator().next()),
        PathUtils.getPathWithoutSchemeAndAuthority(this.testDataPathDummyPath));
  }

  @AfterClass
  public void after() throws Exception {
    this.fs.delete(this.testDataPathDummyPath, true);
  }
}
