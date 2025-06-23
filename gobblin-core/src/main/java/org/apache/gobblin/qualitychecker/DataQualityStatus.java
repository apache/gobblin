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

package org.apache.gobblin.qualitychecker;

import lombok.extern.slf4j.Slf4j;


/**
 * An enumeration for possible statuses for Data quality checks.
 * Its values will be:
 * - PASSED: When all data quality checks pass
 * - FAILED: When any data quality check fails
 * - NOT_EVALUATED: When data quality check evaluation is not performed
 */
@Slf4j
public enum DataQualityStatus {
  PASSED,
  FAILED,
  NOT_EVALUATED,
  UNKNOWN;

  public static DataQualityStatus fromString(String value) {
    if (value == null) {
      return NOT_EVALUATED;
    }
    try {
      return DataQualityStatus.valueOf(value.toUpperCase());
    } catch (IllegalArgumentException e) {
      log.error("Invalid DataQualityStatus value: {}. Returning UNKNOWN.", value, e);
      return UNKNOWN;
    }
  }
}
