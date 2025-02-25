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

package org.apache.gobblin.temporal.ddm.work;

import java.util.Set;

import lombok.AccessLevel;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;


/**
 * Data structure representing the result of generating work units, where it returns the number of generated work units and
 * the folders should be cleaned up after the full job execution is completed
 */
@Data
@Setter(AccessLevel.NONE) // NOTE: non-`final` members solely to enable deserialization
@NoArgsConstructor // IMPORTANT: for jackson (de)serialization
@RequiredArgsConstructor
public class GenerateWorkUnitsResult {
  // NOTE: `@NonNull` to include field in `@RequiredArgsConstructor`, despite - "warning: @NonNull is meaningless on a primitive... @RequiredArgsConstructor"
  @NonNull private int generatedWuCount;
  // TODO: characterize the WUs more thoroughly, by also including destination info, and with more specifics, like src+dest location, I/O config, throttling...
  @NonNull private String sourceClass;
  @NonNull private WorkUnitsSizeSummary workUnitsSizeSummary;
  // Resources that the Temporal Job Launcher should clean up for Gobblin temporary work directory paths in writers
  @NonNull private Set<String> workDirPathsToDelete;
}
