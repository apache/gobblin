package org.apache.gobblin.instrumented;

import java.util.Collection;

import org.apache.gobblin.metrics.ContextAwareCounter;
import org.apache.gobblin.metrics.ContextAwareGauge;


public interface StandardMetricsBridge extends Instrumentable {

  StandardMetrics getStandardMetrics();

  interface StandardMetrics {
    String getName();
    Collection<ContextAwareGauge<?>> getGauges();
    Collection<ContextAwareCounter> getConters();
    Collection<ContextAwareCounter> getMeters();
  }
}
