/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.metrics;

/**
 * An enumeration of metric name suffices.
 *
 * @author ynli
 */
public enum MetricNameSuffix {
  MIN_VALUE("MinValue"),
  MAX_VALUE("MaxValue"),
  MEDIAN_VALUE("MedianValue"),
  MEAN_VALUE("MeanValue"),
  STDDEV_VALUE("StdDevValue"),
  MEAN_EVENT_RATE("MeanEventRate"),
  MIN_DURATION("MinDuration"),
  MAX_DURATION("MaxDuration"),
  MEDIAN_DURATION("MedianDuration"),
  MEAN_DURATION("MeanDuration"),
  STDDEV_DURATION("StdDevDuration"),
  NONE("");

  private final String suffix;

  private MetricNameSuffix(String suffix) {
    this.suffix = suffix;
  }

  /**
   * Get a succinct representation of this {@link MetricNameSuffix}.
   *
   * @return succinct representation of this {@link MetricNameSuffix}
   */
  public String getSuffix() {
    return this.suffix;
  }
}
