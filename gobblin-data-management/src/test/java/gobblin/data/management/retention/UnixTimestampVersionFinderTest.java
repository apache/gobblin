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
    props.put(WatermarkDatasetVersionFinder.WATERMARK_REGEX_KEY, "watermark-([0-9]*)-[a-z]*");

    UnixTimestampVersionFinder parser = new UnixTimestampVersionFinder(null, props);

    DateTime time = new DateTime(2015,1,2,10,15);

    Assert.assertEquals(parser.versionClass(), TimestampedDatasetVersion.class);
    Assert.assertEquals(parser.globVersionPattern(), new Path("*"));
    Assert.assertEquals(parser.getDatasetVersion(new Path("watermark-" + time.getMillis() + "-test"),
        new Path("fullPath")).getDateTime(), time);
  }

}
