/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
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

import static org.hamcrest.MatcherAssert.assertThat;
import gobblin.data.management.retention.policy.TimeBasedRetentionPolicy;
import gobblin.data.management.retention.version.TimestampedDatasetVersion;

import java.util.List;
import java.util.Properties;

import org.apache.commons.collections.ListUtils;
import org.apache.hadoop.fs.Path;
import org.hamcrest.Matchers;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;


import com.google.common.collect.ImmutableList;


public class TimeBasedRetentionPolicyTest {

  @Test
  public void testDefaultRetention() {

    Properties props = new Properties();

    TimeBasedRetentionPolicy policy = new TimeBasedRetentionPolicy(props);

    DateTimeUtils.setCurrentMillisFixed(new DateTime(2015, 6, 2, 18, 0, 0, 0).getMillis());

    TimestampedDatasetVersion datasetVersion1 =
        new TimestampedDatasetVersion(new DateTime(2015, 6, 2, 10, 0, 0, 0), new Path("test"));
    TimestampedDatasetVersion datasetVersion2 =
        new TimestampedDatasetVersion(new DateTime(2015, 6, 1, 10, 0, 0, 0), new Path("test"));

    Assert.assertEquals(policy.versionClass(), TimestampedDatasetVersion.class);

    List<TimestampedDatasetVersion> versions = Lists.newArrayList();
    versions.add(datasetVersion1);
    versions.add(datasetVersion2);
    List<TimestampedDatasetVersion> deletableVersions = Lists.newArrayList(policy.listDeletableVersions(versions));
    Assert.assertEquals(deletableVersions.size(), 1);
    Assert.assertEquals(deletableVersions.get(0).getDateTime(), new DateTime(2015, 6, 1, 10, 0, 0, 0));

    DateTimeUtils.setCurrentMillisSystem();
  }

  @Test(dependsOnMethods = { "testDefaultRetention" })
  public void testISORetentionDuration() throws Exception {

    DateTimeUtils.setCurrentMillisFixed(new DateTime(2016, 2, 11, 10, 0, 0, 0).getMillis());

    // 20 Days
    verify("P20D",
        ImmutableList.of(WithDate(new DateTime(2016, 1, 5, 10, 0, 0, 0)), WithDate(new DateTime(2016, 1, 6, 10, 0, 0, 0))),
        ImmutableList.of(WithDate(new DateTime(2016, 2, 10, 10, 0, 0, 0)), WithDate(new DateTime(2016, 2, 11, 10, 0, 0, 0))));

    // 2 Months
    verify("P2M",
        ImmutableList.of(WithDate(new DateTime(2015, 12, 5, 10, 0, 0, 0)), WithDate(new DateTime(2015, 11, 5, 10, 0, 0, 0))),
        ImmutableList.of(WithDate(new DateTime(2016, 2, 10, 10, 0, 0, 0)), WithDate(new DateTime(2016, 1, 10, 10, 0, 0, 0))));

    // 2 Years
    verify("P2Y",
        ImmutableList.of(WithDate(new DateTime(2014, 1, 5, 10, 0, 0, 0)), WithDate(new DateTime(2013, 1, 5, 10, 0, 0, 0))),
        ImmutableList.of(WithDate(new DateTime(2016, 2, 10, 10, 0, 0, 0)), WithDate(new DateTime(2015, 2, 10, 10, 0, 0, 0))));

    // 20 Hours
    verify("PT20H",
        ImmutableList.of(WithDate(new DateTime(2016, 2, 10, 11, 0, 0, 0)), WithDate(new DateTime(2016, 2, 9, 11, 0, 0, 0))),
        ImmutableList.of(WithDate(new DateTime(2016, 2, 11, 8, 0, 0, 0)), WithDate(new DateTime(2016, 2, 11, 9, 0, 0, 0))));

    DateTimeUtils.setCurrentMillisSystem();
  }

  private static TimestampedDatasetVersion WithDate(DateTime dt){
    return new TimestampedDatasetVersion(dt, new Path("test"));
  }

  private void verify(String duration, List<TimestampedDatasetVersion> toBeDeleted,
      List<TimestampedDatasetVersion> toBeRetained) {

    @SuppressWarnings("unchecked")
    List<TimestampedDatasetVersion> allVersions = ListUtils.union(toBeRetained, toBeDeleted);

    List<TimestampedDatasetVersion> deletableVersions =
        Lists.newArrayList(new TimeBasedRetentionPolicy(duration).listDeletableVersions(allVersions));

    assertThat(deletableVersions, Matchers.containsInAnyOrder(toBeDeleted.toArray()));
    assertThat(deletableVersions, Matchers.not(Matchers.containsInAnyOrder(toBeRetained.toArray())));

  }
}
