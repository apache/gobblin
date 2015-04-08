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
   * @return the fully-qualified name of the metric
   */
  public String getFullyQualifiedName();

  /**
   * Get the {@link MetricContext} the metric is registered in.
   */
  public MetricContext getContext();
}
