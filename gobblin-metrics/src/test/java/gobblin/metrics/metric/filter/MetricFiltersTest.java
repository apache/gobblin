package gobblin.metrics.metric.filter;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;

import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for {@link MetricFilters}.
 */
@Test
public class MetricFiltersTest {

  @Test
  public void andTest() {
    MetricFilter trueMetricFilter = mock(MetricFilter.class);
    when(trueMetricFilter.matches(any(String.class), any(Metric.class))).thenReturn(true);

    MetricFilter falseMetricFilter = mock(MetricFilter.class);
    when(falseMetricFilter.matches(any(String.class), any(Metric.class))).thenReturn(false);

    Assert.assertTrue(MetricFilters.and(trueMetricFilter, trueMetricFilter).matches("", mock(Metric.class)));
    Assert.assertFalse(MetricFilters.and(trueMetricFilter, falseMetricFilter).matches("", mock(Metric.class)));
    Assert.assertFalse(MetricFilters.and(falseMetricFilter, trueMetricFilter).matches("", mock(Metric.class)));
    Assert.assertFalse(MetricFilters.and(falseMetricFilter, falseMetricFilter).matches("", mock(Metric.class)));
  }
}
