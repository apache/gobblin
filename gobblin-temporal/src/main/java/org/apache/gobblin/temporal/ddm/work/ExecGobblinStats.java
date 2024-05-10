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
import java.util.List;
import java.util.Map;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import org.apache.gobblin.runtime.DatasetTaskSummary;


/** Capture details (esp. for the temporal UI) of a {@link org.apache.gobblin.temporal.ddm.workflow.ExecuteGobblinWorkflow} execution */
@Data
@NoArgsConstructor // IMPORTANT: for jackson (de)serialization
@RequiredArgsConstructor
public class ExecGobblinStats {
  // TODO - currently demonstration only: decide upon meaningful details to provide - for example...
  @NonNull private int numWorkUnits;
  @NonNull private int numSuccessful;
  @NonNull private String user;
  @NonNull private Map<String, DatasetStats> stats;

  public static Map<String, DatasetStats> fromDatasetTaskSummary(List<DatasetTaskSummary> datasetTaskSummaries) {
    Map<String, DatasetStats> datasetStatsMap = new HashMap<>();
    for (DatasetTaskSummary datasetTaskSummary : datasetTaskSummaries) {
      datasetStatsMap.put(datasetTaskSummary.getDatasetUrn(),
          new DatasetStats(datasetTaskSummary.getRecordsWritten(), datasetTaskSummary.getBytesWritten(), datasetTaskSummary.isSuccessfullyCommitted()));
    }
    return datasetStatsMap;
  }
}

