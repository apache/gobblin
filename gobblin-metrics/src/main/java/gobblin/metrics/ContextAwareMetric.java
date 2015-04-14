package gobblin.metrics;

import com.codahale.metrics.Metric;


/**
 * An interface for a type of {@link com.codahale.metrics.Metric}s that are aware of their
 * {@link MetricContext} and can have associated {@link Tag}s.
 *
 * @author ynli
 */
public interface ContextAwareMetric extends Metric, Taggable {

  /**
   * Get the name of the metric.
   *
   * @return the name of the metric
   */
  public String getName();

  /**
   * Get the fully-qualified name of the metric.
   *
   * <p>
   *   See {@link Taggable#metricNamePrefix(boolean)} for the semantic of {@code includeTagKeys}.
   * </p>
   *
   * @param includeTagKeys whether to include tag keys in the metric name prefix
   * @return the fully-qualified name of the metric
   */
  public String getFullyQualifiedName(boolean includeTagKeys);

  /**
   * Get the {@link MetricContext} the metric is registered in.
   */
  public MetricContext getContext();
}
