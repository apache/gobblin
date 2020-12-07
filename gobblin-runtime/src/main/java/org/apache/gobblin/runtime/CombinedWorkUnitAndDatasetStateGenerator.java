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
package org.apache.gobblin.runtime;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.gobblin.configuration.CombinedWorkUnitAndDatasetState;
import org.apache.gobblin.configuration.CombinedWorkUnitAndDatasetStateFunctional;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.metastore.DatasetStateStore;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;


/**
 * A class that returns previous {@link WorkUnitState}s and {@link JobState.DatasetState}s from the state store
 * as a {@link CombinedWorkUnitAndDatasetState}.
 */

public class CombinedWorkUnitAndDatasetStateGenerator implements CombinedWorkUnitAndDatasetStateFunctional {
  private DatasetStateStore datasetStateStore;
  private String jobName;

  /**
   * Constructor.
   *
   * @param datasetStateStore the dataset state store
   * @param jobName the job name
   */
  public CombinedWorkUnitAndDatasetStateGenerator(DatasetStateStore datasetStateStore, String jobName) {
    this.datasetStateStore = datasetStateStore;
    this.jobName = jobName;
  }

  @Override
  public CombinedWorkUnitAndDatasetState getCombinedWorkUnitAndDatasetState(String datasetUrn)
      throws Exception {
    Map<String, JobState.DatasetState> datasetStateMap = ImmutableMap.of();
    List<WorkUnitState> workUnitStates = new ArrayList<>();
    if (Strings.isNullOrEmpty(datasetUrn)) {
      datasetStateMap = this.datasetStateStore.getLatestDatasetStatesByUrns(this.jobName);
      workUnitStates = JobState.workUnitStatesFromDatasetStates(datasetStateMap.values());
    } else {
      JobState.DatasetState datasetState =
          (JobState.DatasetState) this.datasetStateStore.getLatestDatasetState(this.jobName, datasetUrn);
      if (datasetState != null) {
        datasetStateMap = ImmutableMap.of(datasetUrn, datasetState);
        workUnitStates = JobState.workUnitStatesFromDatasetStates(Arrays.asList(datasetState));
      }
    }
    return new CombinedWorkUnitAndDatasetState(workUnitStates, datasetStateMap);
  }
}
