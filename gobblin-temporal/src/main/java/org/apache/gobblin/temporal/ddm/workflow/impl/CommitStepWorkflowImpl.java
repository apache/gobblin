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

import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.workflow.Workflow;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.runtime.DatasetTaskSummary;
import org.apache.gobblin.runtime.util.GsonUtils;
import org.apache.gobblin.temporal.ddm.activity.CommitActivity;
import org.apache.gobblin.temporal.ddm.work.CommitStats;
import org.apache.gobblin.temporal.ddm.work.DatasetStats;
import org.apache.gobblin.temporal.ddm.work.WUProcessingSpec;
import org.apache.gobblin.temporal.ddm.workflow.CommitStepWorkflow;
import org.apache.gobblin.temporal.workflows.metrics.TemporalEventTimer;


@Slf4j
public class CommitStepWorkflowImpl implements CommitStepWorkflow {

  private static final RetryOptions ACTIVITY_RETRY_OPTS = RetryOptions.newBuilder()
      .setInitialInterval(Duration.ofSeconds(3))
      .setMaximumInterval(Duration.ofSeconds(100))
      .setBackoffCoefficient(2)
      .setMaximumAttempts(4)
      .build();

  private static final ActivityOptions ACTIVITY_OPTS = ActivityOptions.newBuilder()
      .setStartToCloseTimeout(Duration.ofSeconds(999))
      .setRetryOptions(ACTIVITY_RETRY_OPTS)
      .build();

  private final CommitActivity activityStub = Workflow.newActivityStub(CommitActivity.class, ACTIVITY_OPTS);

  @Override
  public CommitStats commit(WUProcessingSpec workSpec) {
    CommitStats commitGobblinStats = activityStub.commit(workSpec);
    TemporalEventTimer.Factory timerFactory = new TemporalEventTimer.Factory(workSpec.getEventSubmitterContext());
    timerFactory.create(TimingEvent.LauncherTimings.JOB_SUMMARY)
        .withMetadata(TimingEvent.DATASET_TASK_SUMMARIES, GsonUtils.GSON_WITH_DATE_HANDLING.toJson(convertDatasetStatsToTaskSummaries(commitGobblinStats.getDatasetStats())))
        .submit();
    return commitGobblinStats;
  }

  private List<DatasetTaskSummary> convertDatasetStatsToTaskSummaries(Map<String, DatasetStats> datasetStats) {
    List<DatasetTaskSummary> datasetTaskSummaries = new ArrayList<>();
    for (Map.Entry<String, DatasetStats> entry : datasetStats.entrySet()) {
      datasetTaskSummaries.add(new DatasetTaskSummary(entry.getKey(), entry.getValue().getRecordsWritten(), entry.getValue().getBytesWritten(), entry.getValue().isSuccessfullyCommitted()));
    }
    return datasetTaskSummaries;
  }
}
