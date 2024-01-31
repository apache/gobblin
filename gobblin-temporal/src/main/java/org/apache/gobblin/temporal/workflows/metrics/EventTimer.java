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
package org.apache.gobblin.temporal.workflows.metrics;

import java.io.Closeable;

import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.metrics.event.TimingEvent;


/**
 * <p> A timer that can be used to track the duration of an event. This event differs from the {@link TimingEvent} in that
 * this class is not meant to be used outside of {@link io.temporal.workflow.Workflow} code. We cannot use {@link TimingEvent}
 * because it is not serializable and cannot be passed to {@link io.temporal.workflow.Workflow} code due to the
 * {@link EventSubmitter} field. It also relies on {@link System#currentTimeMillis()} which not compatible with {@link io.temporal.workflow.Workflow}
 * since {@link System#currentTimeMillis()} is not deterministic.</p>
 *
 * <p> {@link EventSubmitter} is not easily serializable because the {@link MetricContext} field
 * contains bi-directional relationships via the {@link org.apache.gobblin.metrics.InnerGauge}. Although it's possible
 * to write a custom serializer for {@link EventSubmitter}, it creates a non-obvious sleight of hand where the EventSubmitter
 * metadata will change when crossing {@link io.temporal.workflow.Workflow} or {@link io.temporal.activity.Activity} boundaries. </p>
 *
 * <p> It differs from {@link Closeable} because the close method does not throw {@link java.io.IOException}. {@link TimingEvent}
 * does this but the issue is it does not implement an interface. Inheritance is not a good solution either because of the
 * {@link EventSubmitter} member variable. </p>
 */
public interface EventTimer extends Closeable {
  /**
   * Add additional metadata that will be used for post-processing when the timer is stopped via {@link #stop()}
   * @param key
   * @param metadata
   */
  void addMetadata(String key, String metadata);

  /**
   * Stops the timer and execute any post-processing (e.g. event submission)
   */
  void stop();

  default void close() {
    stop();
  }
}
