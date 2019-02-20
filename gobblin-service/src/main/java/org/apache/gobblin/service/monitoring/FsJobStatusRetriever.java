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

package org.apache.gobblin.service.monitoring;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterators;
import com.typesafe.config.Config;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.metastore.FileContextBasedFsStateStore;
import org.apache.gobblin.metastore.FileContextBasedFsStateStoreFactory;
import org.apache.gobblin.metastore.FsStateStore;
import org.apache.gobblin.metrics.event.TimingEvent;


/**
 * A FileSystem based implementation of {@link JobStatusRetriever}. This implementation stores the job statuses
 * as {@link org.apache.gobblin.configuration.State} objects in a {@link FsStateStore}.
 * The store name is set to flowGroup.flowName, while the table name is set to flowExecutionId.jobGroup.jobName.
 */
@Slf4j
public class FsJobStatusRetriever extends JobStatusRetriever {
  public static final String CONF_PREFIX = "fsJobStatusRetriever";

  @Getter
  private final FileContextBasedFsStateStore<State> stateStore;

  public FsJobStatusRetriever(Config config) {
    this.stateStore = (FileContextBasedFsStateStore<State>) new FileContextBasedFsStateStoreFactory().
        createStateStore(config.getConfig(CONF_PREFIX), State.class);
  }

  @Override
  public Iterator<JobStatus> getJobStatusesForFlowExecution(String flowName, String flowGroup, long flowExecutionId) {
    Preconditions.checkArgument(flowName != null, "FlowName cannot be null");
    Preconditions.checkArgument(flowGroup != null, "FlowGroup cannot be null");

    Predicate<String> flowExecutionIdPredicate = input -> input.startsWith(String.valueOf(flowExecutionId) + ".");
    String storeName = Joiner.on(JobStatusRetriever.STATE_STORE_KEY_SEPARATION_CHARACTER).join(flowGroup, flowName);
    try {
      List<JobStatus> jobStatuses = new ArrayList<>();
      List<String> tableNames = this.stateStore.getTableNames(storeName, flowExecutionIdPredicate);
      for (String tableName: tableNames) {
        List<State> jobStates = this.stateStore.getAll(storeName, tableName);
        if (jobStates.isEmpty()) {
          return Iterators.emptyIterator();
        }
        if (!shouldFilterJobStatus(tableNames, tableName)) {
          jobStatuses.add(getJobStatus(jobStates.get(0)));
        }
      }
      return jobStatuses.iterator();
    } catch (IOException e) {
      log.error("IOException encountered when retrieving job statuses for flow: {},{},{}", flowGroup, flowName, flowExecutionId, e);
      return Iterators.emptyIterator();
    }
  }

  @Override
  public Iterator<JobStatus> getJobStatusesForFlowExecution(String flowName, String flowGroup, long flowExecutionId,
      String jobName, String jobGroup) {
    Preconditions.checkArgument(flowName != null, "flowName cannot be null");
    Preconditions.checkArgument(flowGroup != null, "flowGroup cannot be null");
    Preconditions.checkArgument(jobName != null, "jobName cannot be null");
    Preconditions.checkArgument(jobGroup != null, "jobGroup cannot be null");

    try {
      String storeName = Joiner.on(JobStatusRetriever.STATE_STORE_KEY_SEPARATION_CHARACTER).join(flowGroup, flowName);
      String tableName = Joiner.on(JobStatusRetriever.STATE_STORE_KEY_SEPARATION_CHARACTER).join(flowExecutionId, jobGroup, jobName,
          KafkaJobStatusMonitor.STATE_STORE_TABLE_SUFFIX);
      List<State> jobStates = this.stateStore.getAll(storeName, tableName);
      if (jobStates.isEmpty()) {
        return Iterators.emptyIterator();
      } else {
        return Iterators.singletonIterator(getJobStatus(jobStates.get(0)));
      }
    } catch (IOException e) {
      log.error("Exception encountered when listing files", e);
      return Iterators.emptyIterator();
    }
  }

  /**
   * @param flowName
   * @param flowGroup
   * @return the latest flow execution id with the given flowName and flowGroup. -1 will be returned if no such execution found.
   */
  @Override
  public long getLatestExecutionIdForFlow(String flowName, String flowGroup) {
    Preconditions.checkArgument(flowName != null, "flowName cannot be null");
    Preconditions.checkArgument(flowGroup != null, "flowGroup cannot be null");
    try {
      String storeName = Joiner.on(JobStatusRetriever.STATE_STORE_KEY_SEPARATION_CHARACTER).join(flowGroup, flowName);
      List<String> tableNames = this.stateStore.getTableNames(storeName, input -> true);
      if (tableNames.isEmpty()) {
        return -1L;
      }
      Collections.sort(tableNames);
      return getExecutionIdFromTableName(tableNames.get(tableNames.size() - 1));
    } catch (Exception e) {
      return -1L;
    }
  }

  /**
   *
   * @param jobState instance of {@link State}
   * @return deserialize {@link State} into a {@link JobStatus}.
   */
  private JobStatus getJobStatus(State jobState) {
    String flowGroup = jobState.getProp(TimingEvent.FlowEventConstants.FLOW_GROUP_FIELD);
    String flowName = jobState.getProp(TimingEvent.FlowEventConstants.FLOW_NAME_FIELD);
    long flowExecutionId = Long.parseLong(jobState.getProp(TimingEvent.FlowEventConstants.FLOW_EXECUTION_ID_FIELD));
    String jobName = jobState.getProp(TimingEvent.FlowEventConstants.JOB_NAME_FIELD);
    String jobGroup = jobState.getProp(TimingEvent.FlowEventConstants.JOB_GROUP_FIELD);
    long jobExecutionId = Long.parseLong(jobState.getProp(TimingEvent.FlowEventConstants.JOB_EXECUTION_ID_FIELD, "0"));
    String eventName = jobState.getProp(JobStatusRetriever.EVENT_NAME_FIELD);
    long startTime = Long.parseLong(jobState.getProp(TimingEvent.METADATA_START_TIME, "0"));
    long endTime = Long.parseLong(jobState.getProp(TimingEvent.METADATA_END_TIME, "0"));
    String message = jobState.getProp(TimingEvent.METADATA_MESSAGE, "");
    String lowWatermark = jobState.getProp(TimingEvent.FlowEventConstants.LOW_WATERMARK_FIELD, "");
    String highWatermark = jobState.getProp(TimingEvent.FlowEventConstants.HIGH_WATERMARK_FIELD, "");
    long processedCount = Long.parseLong(jobState.getProp(TimingEvent.FlowEventConstants.PROCESSED_COUNT_FIELD, "0"));

    return JobStatus.builder().flowName(flowName).flowGroup(flowGroup).flowExecutionId(flowExecutionId).
        jobName(jobName).jobGroup(jobGroup).jobExecutionId(jobExecutionId).eventName(eventName).
        lowWatermark(lowWatermark).highWatermark(highWatermark).startTime(startTime).endTime(endTime).
        message(message).processedCount(processedCount).build();
  }

  private long getExecutionIdFromTableName(String tableName) {
    return Long.parseLong(Splitter.on(JobStatusRetriever.STATE_STORE_KEY_SEPARATION_CHARACTER).splitToList(tableName).get(0));
  }

  /**
   * A helper method to determine if {@link JobStatus}es for jobs without a jobGroup/jobName should be filtered out.
   * Once a job has been orchestrated, {@link JobStatus}es without a jobGroup/jobName can be filtered out.
   * @param tableNames
   * @param tableName
   * @return
   */
  private boolean shouldFilterJobStatus(List<String> tableNames, String tableName) {
    return tableNames.size() > 1 && JobStatusRetriever.NA_KEY
        .equals(Splitter.on(JobStatusRetriever.STATE_STORE_KEY_SEPARATION_CHARACTER).splitToList(tableName).get(1));
  }
}
