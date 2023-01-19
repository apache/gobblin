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

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.metrics.GaaSObservabilityEventExperimental;
import org.apache.gobblin.runtime.troubleshooter.MultiContextIssueRepository;


/**
 * The default producer for emitting GaaS Observability Events in the KafkaJobStatusMonitor
 * This class does no work and will not create or emit any events
 */
public class NoopGaaSObservabilityEventProducer extends GaaSObservabilityEventProducer {

  public NoopGaaSObservabilityEventProducer(State state, MultiContextIssueRepository issueRepository, boolean instrumentationEnabled) {
    super(state, issueRepository, instrumentationEnabled);
  }

  public NoopGaaSObservabilityEventProducer() {
    super(null, null, false);
  }

  @Override
  public void emitObservabilityEvent(State jobState) {}

  @Override
  protected void sendUnderlyingEvent(GaaSObservabilityEventExperimental event) {}
}
