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

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.typesafe.config.Config;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.metastore.FileContextBasedFsStateStore;
import org.apache.gobblin.metastore.FileContextBasedFsStateStoreFactory;
import org.apache.gobblin.runtime.spec_executorInstance.LocalFsSpecProducer;
import org.apache.gobblin.service.ExecutionStatus;


import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LocalFsJobStatusRetriever extends JobStatusRetriever {

  public static final String CONF_PREFIX = "localFsJobStatusRetriever";
  private String specProducerPath;

  @Getter
  private final FileContextBasedFsStateStore<State> stateStore;


  // Do not use a state store for this implementation, just look at the job folder that @LocalFsSpecProducer writes to
  public LocalFsJobStatusRetriever(Config config) {
    this.specProducerPath = config.getString(LocalFsSpecProducer.LOCAL_FS_PRODUCER_PATH_KEY);
  }

  private boolean isJobDone(String flowName, String flowGroup) {
    // Local FS has no monitor to update job state yet, for now check if standalone is completed with job, and mark as done
    // Otherwise the job is pending
    try {
      String jobName = LocalFsSpecProducer.getJobFileName(new URI(flowGroup + File.separatorChar + flowName));
      File jobDone = new File(this.specProducerPath + File.separatorChar + jobName + ".done");
      return jobDone.exists();
    } catch (URISyntaxException e) {
      log.error("URISyntaxException occurred when retrieving job status for flow: {},{}", flowGroup, flowName, e);
    }
    return false;
  }

  @Override
  public Iterator<JobStatus> getJobStatusesForFlowExecution(String flowName, String flowGroup, long flowExecutionId) {
    Preconditions.checkArgument(flowName != null, "FlowName cannot be null");
    Preconditions.checkArgument(flowGroup != null, "FlowGroup cannot be null");

    // Instead of looking directly at state store, first check the local FS job workspace to determine if the job is finished
    // Then update the state store with the correct state, instead of having a monitor watch the file system
    // Finally, return the corresponding job state

    Predicate<String> flowExecutionIdPredicate = input -> input.startsWith(String.valueOf(flowExecutionId) + ".");
    String storeName = KafkaJobStatusMonitor.jobStatusStoreName(flowGroup, flowName);
    try {
      List<JobStatus> jobStatuses = new ArrayList<>();
      List<String> tableNames = this.stateStore.getTableNames(storeName, flowExecutionIdPredicate);
      for (String tableName : tableNames) {
        List<State> jobStates = this.stateStore.getAll(storeName, tableName);
        if (jobStates.isEmpty()) {
          return Iterators.emptyIterator();
        }
        // this is gobblin-standalone's indicator that the job is completed
        if (isJobDone(flowName, flowGroup)) {
          jobStates.get(0).setProp(JobStatusRetriever.EVENT_NAME_FIELD, ExecutionStatus.COMPLETE.name());
        }
        jobStatuses.add(getJobStatus(jobStates.get(0)));
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
      String storeName = KafkaJobStatusMonitor.jobStatusStoreName(flowGroup, flowName);
      String tableName = KafkaJobStatusMonitor.jobStatusTableName(flowExecutionId, jobGroup, jobName);
      log.info(storeName);
      log.info(tableName);
      List<State> jobStates = this.stateStore.getAll(storeName, tableName);
      if (jobStates.isEmpty()) {
        log.info("I AM EMPTY");
        return Iterators.emptyIterator();
      } else {
        if (isJobDone(flowName, flowGroup)) {
          jobStates.get(0).setProp(JobStatusRetriever.EVENT_NAME_FIELD, ExecutionStatus.COMPLETE.name());
        }
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
