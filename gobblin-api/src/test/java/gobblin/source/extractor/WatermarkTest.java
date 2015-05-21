package gobblin.source.extractor;

import gobblin.configuration.WorkUnitState;
import gobblin.source.workunit.WorkUnit;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Tests for {@link Watermark}, {@link WatermarkInterval}, and {@link WatermarkSerializerHelper}.
 */
@Test(groups = {"gobblin.source.extractor"})
public class WatermarkTest {

  @Test
  public void testWatermarkWorkUnitSerialization() {
    long lowWatermarkValue = 0;
    long expectedHighWatermarkValue = 100;

    TestWatermark lowWatermark = new TestWatermark();
    lowWatermark.setLongWatermark(lowWatermarkValue);

    TestWatermark expectedHighWatermark = new TestWatermark();
    expectedHighWatermark.setLongWatermark(expectedHighWatermarkValue);

    WatermarkInterval watermarkInterval = new WatermarkInterval(lowWatermark, expectedHighWatermark);
    WorkUnit workUnit = new WorkUnit(null, null, watermarkInterval);

    TestWatermark deserializedLowWatermark =
        (TestWatermark) WatermarkSerializerHelper.convertJsonToWatermark(workUnit.getLowWatermark(),
            TestWatermark.class);
    TestWatermark deserializedExpectedHighWatermark =
        (TestWatermark) WatermarkSerializerHelper.convertJsonToWatermark(workUnit.getExpectedHighWatermark(),
            TestWatermark.class);

    Assert.assertEquals(deserializedLowWatermark.getLongWatermark(), lowWatermarkValue);
    Assert.assertEquals(deserializedExpectedHighWatermark.getLongWatermark(), expectedHighWatermarkValue);
  }

  @Test
  public void testWatermarkWorkUnitStateSerialization() {
    long actualHighWatermarkValue = 50;

    TestWatermark actualHighWatermark = new TestWatermark();
    actualHighWatermark.setLongWatermark(actualHighWatermarkValue);

    WorkUnitState workUnitState = new WorkUnitState();
    workUnitState.setActualHighWatermark(actualHighWatermark);

    TestWatermark deserializedActualHighWatermark =
        (TestWatermark) WatermarkSerializerHelper.convertJsonToWatermark(workUnitState.getActualHighWatermark(),
            TestWatermark.class);

    Assert.assertEquals(deserializedActualHighWatermark.getLongWatermark(), actualHighWatermarkValue);
  }
}
