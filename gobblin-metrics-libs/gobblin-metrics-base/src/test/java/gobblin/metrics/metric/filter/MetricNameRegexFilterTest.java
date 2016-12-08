package gobblin.metrics.metric.filter;

import static org.mockito.Mockito.mock;

import com.codahale.metrics.Metric;

import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for {@link MetricNameRegexFilter}.
 */
@Test
public class MetricNameRegexFilterTest {

  @Test
  public void matchesTest() {
    MetricNameRegexFilter metricNameRegexFilter1 = new MetricNameRegexFilter(".*");
    Assert.assertTrue(metricNameRegexFilter1.matches("test1", mock(Metric.class)));
    Assert.assertTrue(metricNameRegexFilter1.matches("test2", mock(Metric.class)));
    Assert.assertTrue(metricNameRegexFilter1.matches("test3", mock(Metric.class)));

    MetricNameRegexFilter metricNameRegexFilter2 = new MetricNameRegexFilter("test1");
    Assert.assertTrue(metricNameRegexFilter2.matches("test1", mock(Metric.class)));
    Assert.assertFalse(metricNameRegexFilter2.matches("test2", mock(Metric.class)));
    Assert.assertFalse(metricNameRegexFilter2.matches("test3", mock(Metric.class)));
  }
}
