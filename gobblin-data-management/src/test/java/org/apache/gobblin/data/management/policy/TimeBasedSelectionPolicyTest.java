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
package org.apache.gobblin.data.management.policy;

import java.util.Properties;

import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.data.management.version.TimestampedDatasetVersion;


/**
 * Test for {@link SelectAfterTimeBasedPolicy}.
 */
@Test(groups = { "gobblin.data.management.policy" })
public class TimeBasedSelectionPolicyTest {
  @Test
  public void testListCopyableVersions() {
    Properties props = new Properties();
    Path dummyPath = new Path("dummy");
    DateTime dt1 = new DateTime().minusDays(8);
    DateTime dt2 = new DateTime().minusDays(6);

    props.put(SelectAfterTimeBasedPolicy.TIME_BASED_SELECTION_LOOK_BACK_TIME_KEY, "7d");
    SelectAfterTimeBasedPolicy policyLookback7Days = new SelectAfterTimeBasedPolicy(props);
    TimestampedDatasetVersion version1 = new TimestampedDatasetVersion(dt1, dummyPath);
    TimestampedDatasetVersion version2 = new TimestampedDatasetVersion(dt2, dummyPath);
    Assert.assertEquals(policyLookback7Days.listSelectedVersions(Lists.newArrayList(version1, version2)).size(), 1);

    props.put(SelectAfterTimeBasedPolicy.TIME_BASED_SELECTION_LOOK_BACK_TIME_KEY, "1h");
    SelectAfterTimeBasedPolicy policyLookback1Hour = new SelectAfterTimeBasedPolicy(props);
    Assert.assertEquals(policyLookback1Hour.listSelectedVersions(Lists.newArrayList(version1, version2)).size(), 0);

    props.put(SelectAfterTimeBasedPolicy.TIME_BASED_SELECTION_LOOK_BACK_TIME_KEY, "9d");
    SelectAfterTimeBasedPolicy policyLookback8Days = new SelectAfterTimeBasedPolicy(props);
    Assert.assertEquals(policyLookback8Days.listSelectedVersions(Lists.newArrayList(version1, version2)).size(), 2);
  }

  @Test
  public void testSelectAfterTimebasedPolicy() {

    Path dummyPath = new Path("dummy");
    DateTime dt1 = new DateTime().minusDays(8);
    DateTime dt2 = new DateTime().minusDays(6);

    Config config =
        ConfigFactory.parseMap(ImmutableMap
            .of(SelectAfterTimeBasedPolicy.TIME_BASED_SELECTION_LOOK_BACK_TIME_KEY, "7d"));
    SelectAfterTimeBasedPolicy policyLookback7Days = new SelectAfterTimeBasedPolicy(config);
    TimestampedDatasetVersion version1 = new TimestampedDatasetVersion(dt1, dummyPath);
    TimestampedDatasetVersion version2 = new TimestampedDatasetVersion(dt2, dummyPath);
    Assert.assertEquals(policyLookback7Days.listSelectedVersions(Lists.newArrayList(version1, version2)).size(), 1);
    Assert.assertEquals(
        Lists.newArrayList(policyLookback7Days.listSelectedVersions(Lists.newArrayList(version1, version2))).get(0),
        version2);

    config =
        ConfigFactory.parseMap(ImmutableMap
            .of(SelectAfterTimeBasedPolicy.TIME_BASED_SELECTION_LOOK_BACK_TIME_KEY, "1h"));
    SelectAfterTimeBasedPolicy policyLookback1Hour = new SelectAfterTimeBasedPolicy(config);
    Assert.assertEquals(policyLookback1Hour.listSelectedVersions(Lists.newArrayList(version1, version2)).size(), 0);

    config =
        ConfigFactory.parseMap(ImmutableMap
            .of(SelectAfterTimeBasedPolicy.TIME_BASED_SELECTION_LOOK_BACK_TIME_KEY, "9d"));
    SelectAfterTimeBasedPolicy policyLookback8Days = new SelectAfterTimeBasedPolicy(config);
    Assert.assertEquals(policyLookback8Days.listSelectedVersions(Lists.newArrayList(version1, version2)).size(), 2);
  }

  @Test
  public void testSelectBeforeTimebasedPolicy() {

    Path dummyPath = new Path("dummy");
    DateTime dt1 = new DateTime().minusDays(8);
    DateTime dt2 = new DateTime().minusDays(6);

    Config config =
        ConfigFactory.parseMap(ImmutableMap
            .of(SelectAfterTimeBasedPolicy.TIME_BASED_SELECTION_LOOK_BACK_TIME_KEY, "7d"));
    SelectBeforeTimeBasedPolicy policyLookback7Days = new SelectBeforeTimeBasedPolicy(config);
    TimestampedDatasetVersion version1 = new TimestampedDatasetVersion(dt1, dummyPath);
    TimestampedDatasetVersion version2 = new TimestampedDatasetVersion(dt2, dummyPath);
    Assert.assertEquals(policyLookback7Days.listSelectedVersions(Lists.newArrayList(version1, version2)).size(), 1);
    Assert.assertEquals(
        Lists.newArrayList(policyLookback7Days.listSelectedVersions(Lists.newArrayList(version1, version2))).get(0),
        version1);

    config =
        ConfigFactory.parseMap(ImmutableMap
            .of(SelectAfterTimeBasedPolicy.TIME_BASED_SELECTION_LOOK_BACK_TIME_KEY, "1h"));
    SelectBeforeTimeBasedPolicy policyLookback1Hour = new SelectBeforeTimeBasedPolicy(config);
    Assert.assertEquals(policyLookback1Hour.listSelectedVersions(Lists.newArrayList(version1, version2)).size(), 2);

    config =
        ConfigFactory.parseMap(ImmutableMap
            .of(SelectAfterTimeBasedPolicy.TIME_BASED_SELECTION_LOOK_BACK_TIME_KEY, "9d"));
    SelectBeforeTimeBasedPolicy policyLookback9Days = new SelectBeforeTimeBasedPolicy(config);
    Assert.assertEquals(policyLookback9Days.listSelectedVersions(Lists.newArrayList(version1, version2)).size(), 0);
  }

  @Test
  public void testSelectBetweenTimebasedPolicy() {

    Path dummyPath = new Path("dummy");
    DateTime dt1 = new DateTime().minusDays(8);
    DateTime dt2 = new DateTime().minusDays(6);

    Config config =
        ConfigFactory.parseMap(ImmutableMap.of(
            SelectBetweenTimeBasedPolicy.TIME_BASED_SELECTION_MAX_LOOK_BACK_TIME_KEY, "7d",
            SelectBetweenTimeBasedPolicy.TIME_BASED_SELECTION_MIN_LOOK_BACK_TIME_KEY, "4d"));

    SelectBetweenTimeBasedPolicy policyLookback7Days = new SelectBetweenTimeBasedPolicy(config);
    TimestampedDatasetVersion version1 = new TimestampedDatasetVersion(dt1, dummyPath);
    TimestampedDatasetVersion version2 = new TimestampedDatasetVersion(dt2, dummyPath);
    Assert.assertEquals(policyLookback7Days.listSelectedVersions(Lists.newArrayList(version1, version2)).size(), 1);
    Assert.assertEquals(
        Lists.newArrayList(policyLookback7Days.listSelectedVersions(Lists.newArrayList(version1, version2))).get(0),
        version2);

    config =
        ConfigFactory.parseMap(ImmutableMap.of(
            SelectBetweenTimeBasedPolicy.TIME_BASED_SELECTION_MAX_LOOK_BACK_TIME_KEY, "9d",
            SelectBetweenTimeBasedPolicy.TIME_BASED_SELECTION_MIN_LOOK_BACK_TIME_KEY, "4d"));
    SelectBetweenTimeBasedPolicy policyLookback9d4d = new SelectBetweenTimeBasedPolicy(config);
    Assert.assertEquals(policyLookback9d4d.listSelectedVersions(Lists.newArrayList(version1, version2)).size(), 2);

    config =
        ConfigFactory.parseMap(ImmutableMap.of(
            SelectBetweenTimeBasedPolicy.TIME_BASED_SELECTION_MAX_LOOK_BACK_TIME_KEY, "4d",
            SelectBetweenTimeBasedPolicy.TIME_BASED_SELECTION_MIN_LOOK_BACK_TIME_KEY, "1d"));
    SelectBetweenTimeBasedPolicy policyLookback4d1d = new SelectBetweenTimeBasedPolicy(config);
    Assert.assertEquals(policyLookback4d1d.listSelectedVersions(Lists.newArrayList(version1, version2)).size(), 0);

    config =
        ConfigFactory.parseMap(ImmutableMap.of(
            SelectBetweenTimeBasedPolicy.TIME_BASED_SELECTION_MAX_LOOK_BACK_TIME_KEY, "7d"));
    SelectBetweenTimeBasedPolicy policyLookback7d0d = new SelectBetweenTimeBasedPolicy(config);
    Assert.assertEquals(policyLookback7d0d.listSelectedVersions(Lists.newArrayList(version1, version2)).size(), 1);
    Assert.assertEquals(
        Lists.newArrayList(policyLookback7d0d.listSelectedVersions(Lists.newArrayList(version1, version2))).get(0),
        version2);

  }
}
