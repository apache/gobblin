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

package org.apache.gobblin.temporal.ddm.workflow.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import io.temporal.failure.ApplicationFailure;
import io.temporal.workflow.Workflow;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.runtime.DatasetTaskSummary;
import org.apache.gobblin.temporal.ddm.activity.ActivityType;
import org.apache.gobblin.temporal.ddm.activity.CommitActivity;
import org.apache.gobblin.temporal.ddm.work.CommitStats;
import org.apache.gobblin.temporal.ddm.work.DatasetStats;
import org.apache.gobblin.temporal.ddm.work.WUProcessingSpec;
import org.apache.gobblin.temporal.ddm.workflow.CommitStepWorkflow;
import org.apache.gobblin.temporal.workflows.metrics.TemporalEventTimer;


@Slf4j
public class CommitStepWorkflowImpl implements CommitStepWorkflow {

  @Override
  public CommitStats commit(WUProcessingSpec workSpec, final Properties props) {
    final CommitActivity activityStub = Workflow.newActivityStub(CommitActivity.class, ActivityType.COMMIT.buildActivityOptions(props, true));
    CommitStats commitGobblinStats = activityStub.commit(workSpec);
    if (!commitGobblinStats.getOptFailure().isPresent() || commitGobblinStats.getNumCommittedWorkUnits() > 0) {
      TemporalEventTimer.Factory timerFactory = new TemporalEventTimer.WithinWorkflowFactory(workSpec.getEventSubmitterContext(), props);
      timerFactory.create(TimingEvent.LauncherTimings.JOB_SUMMARY)
          .withMetadataAsJson(TimingEvent.DATASET_TASK_SUMMARIES, convertDatasetStatsToTaskSummaries(commitGobblinStats.getDatasetStats()))
          .submit();// emit job summary info on both full and partial commit (ultimately for `GaaSJobObservabilityEvent.datasetsMetrics`)
    }
    if (commitGobblinStats.getOptFailure().isPresent()) {
      throw ApplicationFailure.newNonRetryableFailureWithCause(
          String.format("Failed to commit dataset state for some dataset(s)"), commitGobblinStats.getOptFailure().get().getClass().toString(),
          commitGobblinStats.getOptFailure().get());
    }
    return commitGobblinStats;
  }
  private List<DatasetTaskSummary> convertDatasetStatsToTaskSummaries(Map<String, DatasetStats> datasetStats) {
    List<DatasetTaskSummary> datasetTaskSummaries = new ArrayList<>();
    for (Map.Entry<String, DatasetStats> entry : datasetStats.entrySet()) {
      datasetTaskSummaries.add(new DatasetTaskSummary(entry.getKey(), entry.getValue().getRecordsWritten(), entry.getValue().getBytesWritten(), entry.getValue().isSuccessfullyCommitted(), entry.getValue().getDataQualityCheckStatus()));
    }
    log.info("Converted dataset stats to task summaries: {}", datasetTaskSummaries);
    return datasetTaskSummaries;
  }
}
