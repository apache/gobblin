/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.data.management.retention;

import java.util.Properties;

import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.Test;

import gobblin.data.management.retention.version.TimestampedDatasetVersion;
import gobblin.data.management.retention.version.finder.DateTimeDatasetVersionFinder;


public class TimestampedDatasetVersionFinderTest {

  @Test
  public void testVersionParser() {
    Properties props = new Properties();
    props.put(DateTimeDatasetVersionFinder.DATE_TIME_PATTERN_KEY, "yyyy/MM/dd/hh/mm");

    DateTimeDatasetVersionFinder parser = new DateTimeDatasetVersionFinder(null, props);

    Assert.assertEquals(parser.versionClass(), TimestampedDatasetVersion.class);
    Assert.assertEquals(parser.globVersionPattern(), new Path("*/*/*/*/*"));
    Assert.assertEquals(parser.getDatasetVersion(new Path("2015/06/01/10/12"), new Path("fullPath")).getDateTime(),
        new DateTime(2015,6,1,10,12,0,0));
    Assert.assertEquals(parser.getDatasetVersion(new Path("2015/06/01/10/12"), new Path("fullPath")).
            getPathsToDelete().iterator().next(), new Path("fullPath"));
  }

}
