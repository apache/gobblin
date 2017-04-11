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

package gobblin.metrics;


/**
 * A enumeration of different measurements of metrics.
 *
 * @author Yinan Li
 */
public enum Measurements {

  COUNT("count"),
  MIN("min"),
  MAX("max"),
  MEDIAN("median"),
  MEAN("mean"),
  STDDEV("stddev"),
  PERCENTILE_75TH("75thPercentile"),
  PERCENTILE_95TH("95thPercentile"),
  PERCENTILE_98TH("98thPercentile"),
  PERCENTILE_99TH("99thPercentile"),
  PERCENTILE_999TH("999thPercentile"),
  RATE_1MIN("1MinuteRate"),
  RATE_5MIN("5MinuteRate"),
  RATE_15MIN("5MinuteRate"),
  MEAN_RATE("meanRate");

  private final String name;

  Measurements(String name) {
    this.name = name;
  }

  /**
   * Get a succinct name of this {@link Measurements}.
   *
   * @return a succinct name of this {@link Measurements}
   */
  public String getName() {
    return this.name;
  }
}
