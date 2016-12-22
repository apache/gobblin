package gobblin.metrics.metric.filter;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;


/**
 * Implementation of {@link MetricFilter} that takes in a regex, and {@link #matches(String, Metric)} a {@link Metric}
 * if the name of the {@link Metric} matches the regex.
 */
public class MetricNameRegexFilter implements MetricFilter {

  private final Pattern regex;
  private final Matcher matcher;

  public MetricNameRegexFilter(String regex) {
    this.regex = Pattern.compile(regex);
    this.matcher = this.regex.matcher("");
  }

  @Override
  public boolean matches(String name, Metric metric) {
    return this.matcher.reset(name).matches();
  }
}
