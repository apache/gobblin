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

package org.apache.gobblin.service.modules.utils;

import java.net.URI;
import java.util.Map;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;

import javax.inject.Inject;
import javax.inject.Singleton;
import lombok.Data;
import lombok.Setter;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.ContextAwareGauge;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.RootMetricContext;
import org.apache.gobblin.metrics.ServiceMetricNames;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.service.modules.orchestration.Orchestrator;
import org.apache.gobblin.util.ConfigUtils;


/**
 * Class to store flow related metrics shared between the {@link Orchestrator} and {@link FlowCompilationValidationHelper}
 * so we can easily track all flow compilations and skipped flows handled between the two in a common place.
 */
@Singleton
@Data
public class SharedFlowMetricsSingleton {
  protected final MetricContext metricContext;
  private Map<URI, FlowCompiledState> flowGaugeStateBySpecUri = Maps.newHashMap();
  private Optional<Meter> skippedFlowsMeter;

  @Setter
  public static class FlowCompiledState {
    private CompiledState state = CompiledState.UNKNOWN;
  }

  public enum CompiledState {
    FAILED(-1),
    UNKNOWN(0),
    SUCCESSFUL(1),
    SKIPPED(2);

    public final int value;

    CompiledState(int value) {
      this.value = value;
    }
  }

  @Inject
  public SharedFlowMetricsSingleton(Config config) {
    this.metricContext = Instrumented.getMetricContext(ConfigUtils.configToState(config),
        SharedFlowMetricsSingleton.class);
    this.skippedFlowsMeter = Optional.of(metricContext.contextAwareMeter(ServiceMetricNames.SKIPPED_FLOWS));
  }

  /**
   * Adds a new FlowGauge to the metric context if one does not already exist for this flow spec
   */
  public void addFlowGauge(Spec spec, Config flowConfig, String flowGroup, String flowName) {
    // Only register the metric of flows that are scheduled, run once flows should not be tracked indefinitely
    if (!flowGaugeStateBySpecUri.containsKey(spec.getUri())
        && flowConfig.hasPath(ConfigurationKeys.JOB_SCHEDULE_KEY)) {
      String flowCompiledGaugeName = MetricRegistry.name(ServiceMetricNames.GOBBLIN_SERVICE_PREFIX, flowGroup, flowName,
              ServiceMetricNames.COMPILED);
      flowGaugeStateBySpecUri.put(spec.getUri(), new FlowCompiledState());
      ContextAwareGauge<Integer> gauge =
          RootMetricContext.get().newContextAwareGauge(flowCompiledGaugeName,
              () -> flowGaugeStateBySpecUri.get(spec.getUri()).state.value);
      RootMetricContext.get().register(flowCompiledGaugeName, gauge);
    }
  }
  /**
   * Updates the flowgauge related to the spec if the gauge is being tracked for the flow
   * @param spec FlowSpec to be updated
   * @param state desired state to set the gauge
   */
  public void conditionallyUpdateFlowGaugeSpecState(Spec spec, CompiledState state) {
    if (flowGaugeStateBySpecUri.containsKey(spec.getUri())) {
      flowGaugeStateBySpecUri.get(spec.getUri()).setState(state);
    }
  }
}
