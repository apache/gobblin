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
package org.apache.gobblin.temporal.exception;


import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Getter;


/**
 * An exception thrown when a set of dataset URNs fail to be processed.
 */

@JsonIgnoreProperties(ignoreUnknown = true)
public class FailedDatasetUrnsException extends IOException {

  @Getter
  private final Set<String> failedDatasetUrns;

  /**
   * Creates a new instance of this exception with the failed dataset URNs.
   *
   * @param failedDatasetUrns a set containing the URNs of the datasets that failed to process
   */
  public FailedDatasetUrnsException(Set<String> failedDatasetUrns) {
    super("Failed to process the following dataset URNs: " + String.join(",", failedDatasetUrns));
    this.failedDatasetUrns = failedDatasetUrns;
  }

  /**
   * Default constructor for {@code FailedDatasetUrnsException}.
   * <p>
   * This constructor initializes an empty {@link HashSet} for {@code failedDatasetUrns}.
   * It is provided to support frameworks like Jackson that require a no-argument constructor
   * for deserialization purposes.
   * </p>
   * */
  public FailedDatasetUrnsException() {
    super();
    this.failedDatasetUrns = new HashSet<>();
  }
}
