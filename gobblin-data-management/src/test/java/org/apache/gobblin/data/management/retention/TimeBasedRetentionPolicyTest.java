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

package org.apache.gobblin.data.management.retention;

import static org.hamcrest.MatcherAssert.assertThat;

import java.util.List;

import org.apache.commons.collections.ListUtils;
import org.apache.hadoop.fs.Path;
import org.hamcrest.Matchers;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.testng.annotations.AfterGroups;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.apache.gobblin.data.management.retention.policy.TimeBasedRetentionPolicy;
import org.apache.gobblin.data.management.version.TimestampedDatasetVersion;

@Test(groups = { "SystemTimeTests"})
public class TimeBasedRetentionPolicyTest {

  @Test
  public void testISORetentionDuration() throws Exception {

    DateTimeUtils.setCurrentMillisFixed(new DateTime(2016, 2, 11, 10, 0, 0, 0).getMillis());
    try {
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
    }
    finally {
      // Restore time
      DateTimeUtils.setCurrentMillisSystem();
    }

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

  @AfterGroups("SystemTimeTests")
  public void resetSystemCurrentTime() {
    DateTimeUtils.setCurrentMillisSystem();
  }
}
