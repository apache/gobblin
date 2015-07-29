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

import java.util.List;
import java.util.Properties;

import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.beust.jcommander.internal.Lists;

import gobblin.data.management.retention.policy.TimeBasedRetentionPolicy;
import gobblin.data.management.retention.version.TimestampedDatasetVersion;


public class TimeBasedRetentionPolicyTest {

  @Test
  public void test() {

    Properties props = new Properties();

    TimeBasedRetentionPolicy policy = new TimeBasedRetentionPolicy(props);

    DateTimeUtils.setCurrentMillisFixed(new DateTime(2015, 6, 2, 18, 0, 0, 0).getMillis());

    TimestampedDatasetVersion datasetVersion1 = new TimestampedDatasetVersion(new DateTime(2015, 6, 2, 10, 0, 0, 0), new Path("test"));
    TimestampedDatasetVersion datasetVersion2 = new TimestampedDatasetVersion(new DateTime(2015, 6, 1, 10, 0, 0, 0), new Path("test"));

    Assert.assertEquals(policy.versionClass(), TimestampedDatasetVersion.class);

    List<TimestampedDatasetVersion> versions = Lists.newArrayList();
    versions.add(datasetVersion1);
    versions.add(datasetVersion2);
    List<TimestampedDatasetVersion> deletableVersions = Lists.newArrayList(policy.listDeletableVersions(versions));
    Assert.assertEquals(deletableVersions.size(),1);
    Assert.assertEquals(deletableVersions.get(0).getDateTime(),
        new DateTime(2015, 6, 1, 10, 0, 0, 0));


    DateTimeUtils.setCurrentMillisSystem();
  }
}
