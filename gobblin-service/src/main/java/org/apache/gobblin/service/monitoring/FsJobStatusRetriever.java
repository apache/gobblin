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
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.typesafe.config.Config;

import javax.inject.Inject;
import javax.inject.Singleton;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.metastore.FileContextBasedFsStateStore;
import org.apache.gobblin.metastore.FileContextBasedFsStateStoreFactory;
import org.apache.gobblin.metastore.FsStateStore;
import org.apache.gobblin.runtime.troubleshooter.MultiContextIssueRepository;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.function.CheckedExceptionFunction;


/**
 * A FileSystem based implementation of {@link JobStatusRetriever}. This implementation stores the job statuses
 * as {@link org.apache.gobblin.configuration.State} objects in a {@link FsStateStore}.
 * The store name is set to flowGroup.flowName, while the table name is set to flowExecutionId.jobGroup.jobName.
 */
@Slf4j
@Singleton
public class FsJobStatusRetriever extends JobStatusRetriever {
  public static final String CONF_PREFIX = "fsJobStatusRetriever";

  @Getter
  private final FileContextBasedFsStateStore<State> stateStore;

  @Inject
  public FsJobStatusRetriever(Config config, MultiContextIssueRepository issueRepository) {
    super(ConfigUtils.getBoolean(config, ServiceConfigKeys.GOBBLIN_SERVICE_DAG_MANAGER_ENABLED_KEY,
        ServiceConfigKeys.DEFAULT_GOBBLIN_SERVICE_DAG_MANAGER_ENABLED), issueRepository);
    this.stateStore = (FileContextBasedFsStateStore<State>) new FileContextBasedFsStateStoreFactory().
        createStateStore(config.getConfig(CONF_PREFIX), State.class);
  }

  @Override
  public Iterator<JobStatus> getJobStatusesForFlowExecution(String flowName, String flowGroup, long flowExecutionId) {
    Preconditions.checkArgument(flowName != null, "FlowName cannot be null");
    Preconditions.checkArgument(flowGroup != null, "FlowGroup cannot be null");

    Predicate<String> flowExecutionIdPredicate = input -> input.startsWith(flowExecutionId + ".");
    String storeName = KafkaJobStatusMonitor.jobStatusStoreName(flowGroup, flowName);
    try {
      List<String> tableNames = this.stateStore.getTableNames(storeName, flowExecutionIdPredicate);
      List<JobStatus> jobStatuses = new ArrayList<>(tableNames.size());
      for (String tableName: tableNames) {
        List<State> jobStates = this.stateStore.getAll(storeName, tableName);
        if (jobStates.isEmpty()) {
          return Iterators.emptyIterator();
        }
        jobStatuses.add(getJobStatus(jobStates.get(0)));
      }
      return jobStatuses.iterator();
    } catch (IOException e) {
      log.error(String.format("IOException encountered when retrieving job statuses for flow: %s,%s,%s", flowGroup, flowName, flowExecutionId), e);
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
      String storeName = KafkaJobStatusMonitor.jobStatusStoreName(flowGroup, flowName);
      String tableName = KafkaJobStatusMonitor.jobStatusTableName(flowExecutionId, jobGroup, jobName);
      List<State> jobStates = this.stateStore.getAll(storeName, tableName);
      if (jobStates.isEmpty()) {
        return Iterators.emptyIterator();
      } else {
        return Iterators.singletonIterator(getJobStatus(jobStates.get(0)));
      }
    } catch (IOException e) {
      log.error(String.format("Exception encountered when listing files for flow: %s,%s,%s;%s,%s", flowGroup, flowName, flowExecutionId, jobGroup, jobName), e);
      return Iterators.emptyIterator();
    }
  }

  @Override
  public List<FlowStatus> getFlowStatusesForFlowGroupExecutions(String flowGroup, int countJobStatusesPerFlowName) {
    Preconditions.checkArgument(flowGroup != null, "flowGroup cannot be null");
    Preconditions.checkArgument(countJobStatusesPerFlowName > 0,
        "Number of job statuses per flow name must be at least 1 (was: %s).", countJobStatusesPerFlowName);
    try {
      String storeNamePrefix = KafkaJobStatusMonitor.jobStatusStoreName(flowGroup, "");
      List<String> storeNamesForFlowGroup = stateStore.getStoreNames(storeName -> storeName.startsWith(storeNamePrefix));
      List<State> flowGroupExecutionsStates = storeNamesForFlowGroup.stream().flatMap(CheckedExceptionFunction.wrapToUnchecked(storeName ->
          stateStore.getAll(storeName).stream()
      )).collect(Collectors.toList());
      return asFlowStatuses(groupByFlowExecutionAndRetainLatest(flowGroup, flowGroupExecutionsStates, countJobStatusesPerFlowName));
    } catch (IOException | RuntimeException e) { // (latter likely wrapping `IOException` originating within `wrapUnchecked`)
      log.error(String.format("Exception encountered when listing files for flow group: %s", flowGroup), e);
      return ImmutableList.of();
    }
  }

  /**
   * @param flowName
   * @param flowGroup
   * @return the last <code>count</code> flow execution ids with the given flowName and flowGroup. -1 will be returned if no such execution found.
   */
  @Override
  public List<Long> getLatestExecutionIdsForFlow(String flowName, String flowGroup, int count) {
    Preconditions.checkArgument(flowName != null, "flowName cannot be null");
    Preconditions.checkArgument(flowGroup != null, "flowGroup cannot be null");
    Preconditions.checkArgument(count > 0, "Number of execution ids must be at least 1.");
    try {
      String storeName = KafkaJobStatusMonitor.jobStatusStoreName(flowGroup, flowName);
      List<String> tableNames = this.stateStore.getTableNames(storeName, input -> true);
      Set<Long> flowExecutionIds = new TreeSet<>(tableNames.stream()
          .map(KafkaJobStatusMonitor::getExecutionIdFromTableName)
          .collect(Collectors.toList())).descendingSet();
      return ImmutableList.copyOf(Iterables.limit(flowExecutionIds, count));
    } catch (Exception e) {
      return null;
    }
  }
}
