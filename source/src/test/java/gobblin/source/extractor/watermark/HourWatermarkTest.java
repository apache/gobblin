package gobblin.source.extractor.watermark;

import java.util.HashMap;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link HourWatermark}.
 *
 * @author ydai
 */
@Test(groups = { "gobblin.source.extractor.watermark" })
public class HourWatermarkTest {

  /**
   * Test the method getIntervals, when the lowWaterMark is greater than the highWaterMark.
   */
  @Test
  public void testGetIntervalsOnNegDiff() {
    HourWatermark hourwm = new HourWatermark("Hourwatermark", "test");
    long lwm = 20150201060000l;
    long hwm = 20150201020000l;
    int partition = 30;
    int maxInterval = 4;
    Map<Long, Long> results = hourwm.getIntervals(lwm, hwm, partition, maxInterval);
    Assert.assertEquals(results.size(), 0);
  }

  /**
   * Test the method getIntervals, when the lowWaterMark is equal to the highWaterMark.
   */
  @Test
  public void testGetIntervalsOnZeroDiff() {
    HourWatermark datewm = new HourWatermark("Hourwatermark", "test");
    long lwm = 20150201010000l;
    long hwm = 20150201010000l;
    int partition = 30;
    int maxInterval = 4;
    Map<Long, Long> results = datewm.getIntervals(lwm, hwm, partition, maxInterval);
    Map<Long, Long> expected = new HashMap<Long, Long>();
    expected.put(20150201010000l, 20150201010000l);
    Assert.assertEquals(results, expected);
  }

  /**
   * Test the method getIntervals.
   * Test when the number of intervals divided by the partition value is smaller than maxInterval,
   * result intervals should be based on the partition value.
   */
  @Test
  public void testGetIntervalsOnParition() {
    HourWatermark hourwm = new HourWatermark("Hourwatermark", "test");
    long lwm = 20150201010000l;
    long hwm = 20150201050000l;
    //Partition by 2 hours.
    int partition = 2;
    int maxInterval = 4;
    Map<Long, Long> results = hourwm.getIntervals(lwm, hwm, partition, maxInterval);
    Map<Long, Long> expected = new HashMap<Long, Long>();
    expected.put(20150201010000l, 20150201030000l);
    expected.put(20150201040000l, 20150201050000l);
    Assert.assertEquals(results, expected);
  }

  /**
   * Test the method getIntervals.
   * Test when the number of intervals divided by the partition value is greater than maxInterval,
   * the number of result intervals should be equal to the maxInterval.
   */
  @Test
  public void testGetIntervalsOnMaxInterval() {
    HourWatermark hourwm = new HourWatermark("Hourwatermark", "test");
    long lwm = 20150201011111l;
    long hwm = 20150202011111l;
    int partition = 2;
    int maxInterval = 2;
    Map<Long, Long> results = hourwm.getIntervals(lwm, hwm, partition, maxInterval);
    Map<Long, Long> expected = new HashMap<Long, Long>();
    expected.put(20150201010000l, 20150201130000l);
    expected.put(20150201140000l, 20150202010000l);
    Assert.assertEquals(results, expected);
  }

  /**
   * Test the method getIntervals, when taking invalid input of maxIntervals.
   */
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testGetIntervalsOnInvalidInputs() {
    HourWatermark hourwm = new HourWatermark("Hourwatermark", "test");
    long lwm = 20150201011111l;
    long hwm = 20150202111111l;
    int partition = 2;
    int maxInterval = -1;
    hourwm.getIntervals(lwm, hwm, partition, maxInterval);
  }


  /**
   * Test the method getIntervals, when taking invalid input of partitionInterval.
   */
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testGetIntervalsOnInvalidPartitionInterval() {
    HourWatermark hourwm = new HourWatermark("Datewatermark", "test");
    long lwm = 20150201011111l;
    long hwm = 20150206111111l;
    int partition = -1;
    int maxInterval = 2;
    hourwm.getIntervals(lwm, hwm, partition, maxInterval);
  }

}
