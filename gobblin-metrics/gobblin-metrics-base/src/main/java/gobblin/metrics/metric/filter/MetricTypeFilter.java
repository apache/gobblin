package gobblin.metrics.metric.filter;

import java.util.List;

import javax.annotation.Nullable;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import gobblin.metrics.metric.Metrics;


/**
 * Implementation of {@link MetricFilter} that takes in a {@link String} containing a comma separated list of
 * {@link Metrics}. This {@link MetricFilter} {@link #matches(String, Metric)} a {@link Metric} if the given
 * {@link Metric} has a type in the given list of allowed {@link Metrics}.
 */
public class MetricTypeFilter implements MetricFilter {

  private List<Metrics> allowedMetrics;

  public MetricTypeFilter(String allowedMetrics) {
    if (Strings.isNullOrEmpty(allowedMetrics)) {
      this.allowedMetrics = Lists.newArrayList(Metrics.values());
    } else {
      List<String> allowedMetricsList = Splitter.on(",").trimResults().omitEmptyStrings().splitToList(allowedMetrics);
      this.allowedMetrics = Lists.transform(allowedMetricsList, new Function<String, Metrics>() {
        @Nullable @Override public Metrics apply(String input) {
          return input == null ? null : Metrics.valueOf(input);
        }
      });
    }
  }

  @Override
  public boolean matches(String name, Metric metric) {
    final Class<? extends Metric> metricClass = metric.getClass();

    return Iterables.any(this.allowedMetrics, new Predicate<Metrics>() {
      @Override public boolean apply(@Nullable Metrics input) {
        return input != null && input.getMetricClass().isAssignableFrom(metricClass);
      }
    });
  }
}
