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
import org.apache.gobblin.metrics.GaaSObservabilityEventExperimental;
import org.apache.gobblin.runtime.troubleshooter.MultiContextIssueRepository;


/**
 * An extension of GaaSObservabilityEventProducer which creates the events and stores them in a list
 * Tests can use a getter to fetch a read-only version of the events that were emitted
 */
public class MockGaaSObservabilityEventProducer extends GaaSObservabilityEventProducer {
  private List<GaaSObservabilityEventExperimental> emittedEvents = new ArrayList<>();

  public MockGaaSObservabilityEventProducer(State state, MultiContextIssueRepository issueRepository) {
    super(state, issueRepository, false);
  }

  @Override
  protected void sendUnderlyingEvent(GaaSObservabilityEventExperimental event) {
    emittedEvents.add(event);
  }

  /**
   * Returns the events that the mock producer has written
   * This should only be used as a read-only object for emitted GaaSObservabilityEvents
   * @return list of events that would have been emitted
   */
  public List<GaaSObservabilityEventExperimental> getTestEmittedEvents() {
    return Collections.unmodifiableList(this.emittedEvents);
  }
}