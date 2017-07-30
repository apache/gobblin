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

import gobblin.data.management.retention.version.TimestampedDatasetVersion;
import gobblin.data.management.retention.version.finder.UnixTimestampVersionFinder;
import gobblin.data.management.retention.version.finder.WatermarkDatasetVersionFinder;

import java.util.Properties;

import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.Test;


public class UnixTimestampVersionFinderTest {

  @Test
  public void test() {

    Properties props = new Properties();
    props.put(WatermarkDatasetVersionFinder.DEPRECATED_WATERMARK_REGEX_KEY, "watermark-([0-9]*)-[a-z]*");

    UnixTimestampVersionFinder parser = new UnixTimestampVersionFinder(null, props);

    DateTime time = new DateTime(2015,1,2,10,15);

    Assert.assertEquals(parser.versionClass(), TimestampedDatasetVersion.class);
    Assert.assertEquals(parser.globVersionPattern(), new Path("*"));
    Assert.assertEquals(parser.getDatasetVersion(new Path("watermark-" + time.getMillis() + "-test"),
        new Path("fullPath")).getDateTime(), time);
  }

}
