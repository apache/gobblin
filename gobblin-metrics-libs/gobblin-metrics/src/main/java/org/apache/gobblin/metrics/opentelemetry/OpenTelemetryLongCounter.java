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

package org.apache.gobblin.metrics.opentelemetry;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;


/**
 * Implementation of {@link OpenTelemetryMetric} that wraps an OpenTelemetry {@link LongCounter}.
 *
 * <p>This class provides a counter for recording values.
 * It supports adding values with optional additional attributes that can be merged with base attributes.</p>
 *
 */
@Slf4j
@AllArgsConstructor
public class OpenTelemetryLongCounter implements OpenTelemetryMetric {
  private String name;
  private Attributes baseAttributes;
  private LongCounter longCounter;

  /**
   * Adds the specified value to the counter with the base attributes.
   *
   * @param value the value to add to the counter
   */
  public void add(long value) {
    log.debug("Emitting long counter metric: {}, value: {}, attributes: {}", this.name, value, this.baseAttributes);
    this.longCounter.add(value, this.baseAttributes);
  }

  /**
   * Adds the specified value to the counter with a combination of base attributes and additional attributes.
   *
   * @param value the value to add to the counter
   * @param additionalAttributes the additional attributes to be merged with base attributes
   */
  public void add(long value, Attributes additionalAttributes) {
    log.debug("Emitting long counter metric: {}, value: {}, base attributes: {}, additional attributes: {}",
        this.name, value, this.baseAttributes, additionalAttributes);
    this.longCounter.add(value, OpenTelemetryHelper.mergeAttributes(this.baseAttributes, additionalAttributes));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getMetricName() {
    return this.name;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public OpenTelemetryMetricType getMetricType() {
    return OpenTelemetryMetricType.LONG_COUNTER;
  }

  /**
   * Returns a string representation of this counter with its name.
   */
  @Override
  public String toString() {
    return "OpenTelemetryLongCounter{name='" + name + "'}";
  }

}
