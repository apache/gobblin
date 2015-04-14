/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.metrics;

import com.codahale.metrics.Metric;


/**
 * An interface for factory classes for {@link ContextAwareMetric}s.
 *
 * @author ynli
 */
public interface ContextAwareMetricFactory<T extends ContextAwareMetric> {

  public static final ContextAwareMetricFactory<ContextAwareCounter> DEFAULT_CONTEXT_AWARE_COUNTER_FACTORY =
      new ContextAwareCounterFactory();
  public static final ContextAwareMetricFactory<ContextAwareMeter> DEFAULT_CONTEXT_AWARE_METER_FACTORY =
      new ContextAwareMeterFactory();
  public static final ContextAwareMetricFactory<ContextAwareHistogram> DEFAULT_CONTEXT_AWARE_HISTOGRAM_FACTORY =
      new ContextAwareHistogramFactory();
  public static final ContextAwareMetricFactory<ContextAwareTimer> DEFAULT_CONTEXT_AWARE_TIMER_FACTORY =
      new ContextAwareTimerFactory();

  /**
   * Create a new context-aware metric.
   *
   * @param context the {@link MetricContext} of the metric
   * @param name metric name
   * @return the newly created metric
   */
  public T newMetric(MetricContext context, String name);

  /**
   * Check if a given metric is an instance of the type of context-aware metrics created by this
   * {@link ContextAwareMetricFactory}.
   *
   * @param metric the given metric
   * @return {@code true} if the given metric is an instance of the type of context-aware metrics
   *         created by this {@link ContextAwareMetricFactory}, {@code false} otherwise.
   */
  public boolean isInstance(Metric metric);

  /**
   * A default implementation of {@link ContextAwareMetricFactory} for {@link ContextAwareCounter}s.
   */
  public static class ContextAwareCounterFactory implements ContextAwareMetricFactory<ContextAwareCounter> {

    @Override
    public ContextAwareCounter newMetric(MetricContext context, String name) {
      return new ContextAwareCounter(context, name);
    }

    @Override
    public boolean isInstance(Metric metric) {
      return ContextAwareCounter.class.isInstance(metric);
    }
  }

  /**
   * A default implementation of {@link ContextAwareMetricFactory} for {@link ContextAwareMeter}s.
   */
  public static class ContextAwareMeterFactory implements ContextAwareMetricFactory<ContextAwareMeter> {

    @Override
    public ContextAwareMeter newMetric(MetricContext context, String name) {
      return new ContextAwareMeter(context, name);
    }

    @Override
    public boolean isInstance(Metric metric) {
      return ContextAwareMeter.class.isInstance(metric);
    }
  }

  /**
   * A default implementation of {@link ContextAwareMetricFactory} for {@link ContextAwareHistogram}s.
   */
  public static class ContextAwareHistogramFactory implements ContextAwareMetricFactory<ContextAwareHistogram> {

    @Override
    public ContextAwareHistogram newMetric(MetricContext context, String name) {
      return new ContextAwareHistogram(context, name);
    }

    @Override
    public boolean isInstance(Metric metric) {
      return ContextAwareHistogram.class.isInstance(metric);
    }
  }

  /**
   * A default implementation of {@link ContextAwareMetricFactory} for {@link ContextAwareTimer}s.
   */
  public static class ContextAwareTimerFactory implements ContextAwareMetricFactory<ContextAwareTimer> {

    @Override
    public ContextAwareTimer newMetric(MetricContext context, String name) {
      return new ContextAwareTimer(context, name);
    }

    @Override
    public boolean isInstance(Metric metric) {
      return ContextAwareTimer.class.isInstance(metric);
    }
  }
}
