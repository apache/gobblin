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
package gobblin.data.management.policy;

import java.util.Properties;

import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import gobblin.data.management.version.TimestampedDatasetVersion;


/**
 * Test for {@link SelectAfterTimeBasedPolicy}.
 */
@Test(groups = { "gobblin.data.management.policy" })
public class TimeBasedSelectionPolicyTest {
  @Test
  public void testListCopyableVersions() {
    Properties props = new Properties();
    Path dummyPath = new Path("dummy");
    DateTime dt1 = new DateTime().minusDays(7);
    DateTime dt2 = new DateTime().minusDays(6);

    props.put(SelectAfterTimeBasedPolicy.TIME_BASED_SELECTION_LOOK_BACK_TIME_KEY, "7d");
    SelectAfterTimeBasedPolicy copyPolicyLookback7Days = new SelectAfterTimeBasedPolicy(props);
    TimestampedDatasetVersion version1 = new TimestampedDatasetVersion(dt1, dummyPath);
    TimestampedDatasetVersion version2 = new TimestampedDatasetVersion(dt2, dummyPath);
    Assert.assertEquals(copyPolicyLookback7Days.listSelectedVersions(Lists.newArrayList(version1, version2)).size(), 1);

    props.put(SelectAfterTimeBasedPolicy.TIME_BASED_SELECTION_LOOK_BACK_TIME_KEY, "1h");
    SelectAfterTimeBasedPolicy copyPolicyLookback1Hour = new SelectAfterTimeBasedPolicy(props);
    Assert.assertEquals(copyPolicyLookback1Hour.listSelectedVersions(Lists.newArrayList(version1, version2)).size(), 0);

    props.put(SelectAfterTimeBasedPolicy.TIME_BASED_SELECTION_LOOK_BACK_TIME_KEY, "8d");
    SelectAfterTimeBasedPolicy copyPolicyLookback8Days = new SelectAfterTimeBasedPolicy(props);
    Assert.assertEquals(copyPolicyLookback8Days.listSelectedVersions(Lists.newArrayList(version1, version2)).size(), 2);
  }
}
