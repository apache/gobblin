package gobblin.source.extractor.watermark;

import java.util.HashMap;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link DateWatermark}.
 *
 * @author ydai
 */
@Test(groups = { "gobblin.source.extractor.watermark" })
public class DateWatermarkTest {

  /**
   * Test the method getIntervals, when the lowWaterMark is greater than the highWaterMark.
   */
  @Test
  public void testGetIntervalsOnNegDiff() {
    DateWatermark datewm = new DateWatermark("Datewatermark", "test");
    long lwm = 20150206000000l;
    long hwm = 20150201000000l;
    int partition = 30;
    int maxInterval = 4;
    Map<Long, Long> results = datewm.getIntervals(lwm, hwm, partition, maxInterval);
    Assert.assertEquals(results.size(), 0);
  }

  /**
   * Test the method getIntervals, when the lowWaterMark is equal to the highWaterMark.
   */
  @Test
  public void testGetIntervalsOnZeroDiff() {
    DateWatermark datewm = new DateWatermark("Datewatermark", "test");
    long lwm = 20150201000000l;
    long hwm = 20150201000000l;
    int partition = 30;
    int maxInterval = 4;
    Map<Long, Long> results = datewm.getIntervals(lwm, hwm, partition, maxInterval);
    Map<Long, Long> expected = new HashMap<Long, Long>();
    expected.put(20150201000000l, 20150201000000l);
    Assert.assertEquals(results, expected);
  }

  /**
   * Test the method getIntervals.
   * Test when the number of intervals divided by the partition value is smaller than maxInterval,
   * result intervals should be based on the partition value.
   */
  @Test
  public void testGetIntervalsOnParition() {
    DateWatermark datewm = new DateWatermark("Datewatermark", "test");
    long lwm = 20150201000000l;
    long hwm = 20150206000000l;
    //Partition by one day.
    int partition = 30;
    int maxInterval = 4;
    Map<Long, Long> results = datewm.getIntervals(lwm, hwm, partition, maxInterval);
    Map<Long, Long> expected = new HashMap<Long, Long>();
    expected.put(20150201000000l, 20150202000000l);
    expected.put(20150203000000l, 20150204000000l);
    expected.put(20150205000000l, 20150206000000l);
    Assert.assertEquals(results, expected);
  }

  /**
   * Test the method getIntervals.
   * Test when the number of intervals divided by the partition value is greater than maxInterval,
   * the number of result intervals should be equal to the maxInterval.
   */
  @Test
  public void testGetIntervalsOnMaxInterval() {
    DateWatermark datewm = new DateWatermark("Datewatermark", "test");
    long lwm = 20150201000000l;
    long hwm = 20150206000000l;
    int partition = 30;
    int maxInterval = 2;
    Map<Long, Long> results = datewm.getIntervals(lwm, hwm, partition, maxInterval);
    Map<Long, Long> expected = new HashMap<Long, Long>();
    expected.put(20150201000000l, 20150203000000l);
    expected.put(20150204000000l, 20150206000000l);
    Assert.assertEquals(results, expected);
  }

  /**
   * Test the method getIntervals, when taking invalid input of maxIntervals.
   */
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testGetIntervalsOnInvalidMaxInterval() {
    DateWatermark datewm = new DateWatermark("Datewatermark", "test");
    long lwm = 20150201011111l;
    long hwm = 20150206111111l;
    int partition = 30;
    int maxInterval = -1;
    datewm.getIntervals(lwm, hwm, partition, maxInterval);
  }

  /**
   * Test the method getIntervals, when taking invalid input of partitionInterval.
   */
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testGetIntervalsOnInvalidPartitionInterval() {
    DateWatermark datewm = new DateWatermark("Datewatermark", "test");
    long lwm = 20150201011111l;
    long hwm = 20150206111111l;
    int partition = 20;
    int maxInterval = 2;
    datewm.getIntervals(lwm, hwm, partition, maxInterval);
  }

}
