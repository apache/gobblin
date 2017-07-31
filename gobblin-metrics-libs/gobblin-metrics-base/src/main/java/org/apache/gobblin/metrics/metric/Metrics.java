/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gobblin.metrics.metric;

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
