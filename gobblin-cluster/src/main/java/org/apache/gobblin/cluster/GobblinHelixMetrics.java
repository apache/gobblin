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

package org.apache.gobblin.cluster;

import java.util.concurrent.TimeUnit;

import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.instrumented.StandardMetricsBridge;
import org.apache.gobblin.metrics.ContextAwareMeter;
import org.apache.gobblin.metrics.ContextAwareTimer;
import org.apache.gobblin.metrics.MetricContext;


public class GobblinHelixMetrics extends StandardMetricsBridge.StandardMetrics {
  public static final String TIMER_FOR_HELIX_WAIT = "timeForHelixWait";
  public static final String TIMER_FOR_HELIX_SUBMIT = "timeForHelixSubmit";
  public static final String METER_FOR_HELIX_SUBMIT = "meterForHelixSubmit";
  final String metricsName;
  final ContextAwareTimer timeForHelixWait;
  final ContextAwareTimer timeForHelixSubmit;
  final ContextAwareMeter submitMeter;

  public GobblinHelixMetrics(String metricsName, final MetricContext metricContext, int windowSizeInMin) {
    this.metricsName = metricsName;
    this.timeForHelixWait = metricContext.contextAwareTimer(TIMER_FOR_HELIX_WAIT, windowSizeInMin, TimeUnit.MINUTES);
    this.timeForHelixSubmit = metricContext.contextAwareTimer(TIMER_FOR_HELIX_SUBMIT, windowSizeInMin, TimeUnit.MINUTES);
    this.submitMeter = metricContext.contextAwareMeter(METER_FOR_HELIX_SUBMIT);
    this.contextAwareMetrics.add(timeForHelixWait);
    this.contextAwareMetrics.add(timeForHelixSubmit);
    this.contextAwareMetrics.add(submitMeter);
  }

  public void updateTimeForHelixSubmit(long startTime) {
    Instrumented.updateTimer(
        com.google.common.base.Optional.of(this.timeForHelixSubmit),
        System.currentTimeMillis() - startTime, TimeUnit.MILLISECONDS);
  }

  public void updateTimeForHelixWait(long startTime) {
    Instrumented.updateTimer(
        com.google.common.base.Optional.of(this.timeForHelixWait),
        System.currentTimeMillis() - startTime, TimeUnit.MILLISECONDS);
  }

  @Override
  public String getName() {
    return this.metricsName;
  }
}
