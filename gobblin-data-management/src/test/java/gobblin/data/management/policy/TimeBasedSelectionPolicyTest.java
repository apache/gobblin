package gobblin.data.management.policy;

import java.util.Properties;

import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import gobblin.data.management.version.TimestampedDatasetVersion;


/**
 * Test for {@link TimeBasedSelectionPolicy}.
 */
@Test(groups = { "gobblin.data.management.policy" })
public class TimeBasedSelectionPolicyTest {
  @Test
  public void testListCopyableVersions() {
    Properties props = new Properties();
    Path dummyPath = new Path("dummy");
    DateTime dt1 = new DateTime().minusDays(7);
    DateTime dt2 = new DateTime().minusDays(6);

    TimeBasedSelectionPolicy copyPolicyLookback7Days = new TimeBasedSelectionPolicy(props);
    TimestampedDatasetVersion version1 = new TimestampedDatasetVersion(dt1, dummyPath);
    TimestampedDatasetVersion version2 = new TimestampedDatasetVersion(dt2, dummyPath);
    Assert.assertEquals(copyPolicyLookback7Days.listSelectedVersions(Lists.newArrayList(version1, version2)).size(), 1);

    props.put(TimeBasedSelectionPolicy.TIME_BASED_SELECTION_LOOK_BACK_TIME_KEY, "1h");
    TimeBasedSelectionPolicy copyPolicyLookback1Hour = new TimeBasedSelectionPolicy(props);
    Assert.assertEquals(copyPolicyLookback1Hour.listSelectedVersions(Lists.newArrayList(version1, version2)).size(), 0);

    props.put(TimeBasedSelectionPolicy.TIME_BASED_SELECTION_LOOK_BACK_TIME_KEY, "8d");
    TimeBasedSelectionPolicy copyPolicyLookback8Days = new TimeBasedSelectionPolicy(props);
    Assert.assertEquals(copyPolicyLookback8Days.listSelectedVersions(Lists.newArrayList(version1, version2)).size(), 2);
  }
}
