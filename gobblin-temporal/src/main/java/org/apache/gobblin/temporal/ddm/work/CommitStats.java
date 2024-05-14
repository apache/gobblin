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

import java.util.HashMap;
import java.util.Map;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;


/**
 * Data structure representing the stats for a committed dataset, and the total number of committed workunits in the Gobblin Temporal job
 * Return type of {@link org.apache.gobblin.temporal.ddm.workflow.ProcessWorkUnitsWorkflow#process(WUProcessingSpec)}
 * and {@link org.apache.gobblin.temporal.ddm.workflow.CommitStepWorkflow#commit(WUProcessingSpec)}.
 */
@Data
@NoArgsConstructor // IMPORTANT: for jackson (de)serialization
@RequiredArgsConstructor
public class CommitStats {
  @NonNull private Map<String, DatasetStats> datasetStats;
  @NonNull private int numCommittedWorkUnits;

  public static CommitStats createEmpty() {
    return new CommitStats(new HashMap<>(), 0);
  }
}
