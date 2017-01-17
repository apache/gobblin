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

package gobblin.util.limiter;

/**
 * An enumeration of types of {@link Limiter}s supported out-of-the-box.
 *
 * @author Yinan Li
 */
public enum BaseLimiterType {

  /**
   * For {@link RateBasedLimiter}.
   */
  RATE_BASED("rate"),

  /**
   * For {@link TimeBasedLimiter}.
   */
  TIME_BASED("time"),

  /**
   * For {@link CountBasedLimiter}.
   */
  COUNT_BASED("count"),

  /**
   * For {@link PoolBasedLimiter}.
   */
  POOL_BASED("pool");

  private final String name;

  BaseLimiterType(String name) {
    this.name = name;
  }

  @Override
  public String toString() {
    return this.name;
  }

  /**
   * Get a {@link BaseLimiterType} for the given name.
   *
   * @param name the given name
   * @return a {@link BaseLimiterType} for the given name
   */
  public static BaseLimiterType forName(String name) {
    if (name.equalsIgnoreCase(RATE_BASED.name)) {
      return RATE_BASED;
    }
    if (name.equalsIgnoreCase(TIME_BASED.name)) {
      return TIME_BASED;
    }
    if (name.equalsIgnoreCase(COUNT_BASED.name)) {
      return COUNT_BASED;
    }
    if (name.equalsIgnoreCase(POOL_BASED.name)) {
      return POOL_BASED;
    }
    throw new IllegalArgumentException("No Limiter implementation available for name: " + name);
  }
}
