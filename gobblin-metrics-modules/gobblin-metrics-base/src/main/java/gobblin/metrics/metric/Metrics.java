package gobblin.metrics.metric;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.Timer;

import lombok.Getter;


/**
 * An {@link Enum} of all {@link com.codahale.metrics.Metric}s.
 */
public enum Metrics {

  COUNTER(Counter.class),
  GAUGE(Gauge.class),
  HISTOGRAM(Histogram.class),
  METER(Meter.class),
  TIMER(Timer.class);

  @Getter
  private final Class<? extends Metric> metricClass;

  Metrics(Class<? extends Metric> metricClass) {
    this.metricClass = metricClass;
  }
}
