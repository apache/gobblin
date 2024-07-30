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

package org.apache.gobblin.service.monitoring;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.metrics.GaaSFlowObservabilityEvent;
import org.apache.gobblin.metrics.GaaSJobObservabilityEvent;
import org.apache.gobblin.metrics.InMemoryOpenTelemetryMetrics;
import org.apache.gobblin.metrics.OpenTelemetryMetricsBase;
import org.apache.gobblin.runtime.troubleshooter.MultiContextIssueRepository;


/**
 * An extension of GaaSJobObservabilityEventProducer which creates the events and stores them in a list
 * Tests can use a getter to fetch a read-only version of the events that were emitted
 */
public class MockGaaSJobObservabilityEventProducer extends GaaSJobObservabilityEventProducer {
  private List<GaaSJobObservabilityEvent> emittedJobEvents = new ArrayList<>();

  private List<GaaSFlowObservabilityEvent> emittedFlowEvents = new ArrayList<>();

  public MockGaaSJobObservabilityEventProducer(State state, MultiContextIssueRepository issueRepository, boolean instrumentationEnabled) {
    super(state, issueRepository, instrumentationEnabled);
  }

  @Override
  protected OpenTelemetryMetricsBase getOpentelemetryMetrics(State state) {
    return InMemoryOpenTelemetryMetrics.getInstance(state);
  }
  @Override
  protected void sendJobLevelEvent(GaaSJobObservabilityEvent event) {
    emittedJobEvents.add(event);
  }

  @Override
  protected void sendFlowLevelEvent(GaaSFlowObservabilityEvent event) {
    emittedFlowEvents.add(event);
  }

  /**
   * Returns the job level events that the mock producer has written
   * This should only be used as a read-only object for emitted GaaSJobObservabilityEvents
   * @return list of events that would have been emitted
   */
  public List<GaaSJobObservabilityEvent> getTestEmittedJobEvents() {
    return Collections.unmodifiableList(this.emittedJobEvents);
  }

  /**
   * Returns the flow level events that the mock producer has written
   * This should only be used as a read-only object for emitted GaaSFlowObservabilityEvents
   * @return list of events that would have been emitted
   */
  public List<GaaSFlowObservabilityEvent> getTestEmittedFlowEvents() {
    return Collections.unmodifiableList(this.emittedFlowEvents);
  }


  public InMemoryOpenTelemetryMetrics getOpentelemetryMetrics() {
    return (InMemoryOpenTelemetryMetrics) this.opentelemetryMetrics;
  }
}