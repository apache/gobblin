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

package gobblin.source.extractor;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;


/**
 * An enumeration of retry modes determining how the failed work unit should retry
 *
 * @author Chen Guo
 */
public enum WorkUnitRetryMode {
  /**
   * Clear the failed task state before retrying
   */
  FULL("full"),

  /**
   * Continue from last failed state for retry
   */
  FROM_FAILED("from_failed");

  private final String name;

  WorkUnitRetryMode(String name) {
    this.name = name;
  }

  /**
   * Get a {@link WorkUnitRetryMode} of the given name.
   *
   * @param name Work unit retry mode name
   * @return a {@link WorkUnitRetryMode} of the given name
   * @throws IllegalArgumentException if the name does not represent a
   *         valid work unit retry mode
   */
  public static WorkUnitRetryMode forName(String name) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(name));

    if (FULL.name.equalsIgnoreCase(name)) {
      return FULL;
    }

    if (FROM_FAILED.name.equalsIgnoreCase(name)) {
      return FROM_FAILED;
    }
    throw new IllegalArgumentException(String.format("Work unit retry policy with name %s is not supported", name));
  }

  /**
   * @param name provide null or empty string to get the default mode.
   * @return the specified WorkUnitRetryMode, otherwise return FROM_FAILED as the default mode.
   */
  public static WorkUnitRetryMode forNameOrDefault(String name) {
    if (Strings.isNullOrEmpty(name)) {
      return FROM_FAILED;
    }
    return forName(name);
  }
}
