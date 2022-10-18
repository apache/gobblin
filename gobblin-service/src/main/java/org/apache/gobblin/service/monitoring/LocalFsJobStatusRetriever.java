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

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.typesafe.config.Config;

import javax.inject.Inject;
import javax.inject.Singleton;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.metastore.StateStore;
import org.apache.gobblin.runtime.spec_executorInstance.LocalFsSpecProducer;
import org.apache.gobblin.runtime.troubleshooter.MultiContextIssueRepository;
import org.apache.gobblin.service.ExecutionStatus;
import org.apache.gobblin.service.ServiceConfigKeys;


/**
 * A job status monitor for jobs completed by a Gobblin Standalone instance running on the same machine. Mainly used for sandboxing/testing
 * Considers a job done when Gobblin standalone appends ".done" to the job. Otherwise it will assume the job is in progress
 */
@Slf4j
@Singleton
public class LocalFsJobStatusRetriever extends JobStatusRetriever {

  public static final String CONF_PREFIX = "localFsJobStatusRetriever.";
  private final String specProducerPath;

  // Do not use a state store for this implementation, just look at the job folder that @LocalFsSpecProducer writes to
  @Inject
  public LocalFsJobStatusRetriever(Config config, MultiContextIssueRepository issueRepository) {
    super(ServiceConfigKeys.DEFAULT_GOBBLIN_SERVICE_DAG_MANAGER_ENABLED, issueRepository);
    this.specProducerPath = config.getString(CONF_PREFIX + LocalFsSpecProducer.LOCAL_FS_PRODUCER_PATH_KEY);
  }

  private boolean doesJobExist(String flowName, String flowGroup, long flowExecutionId, String suffix) {
    // Local FS has no monitor to update job state yet, for now check if standalone is completed with job, and mark as done
    // Otherwise the job is pending
    try {
      String fileName = LocalFsSpecProducer.getJobFileName(new URI(File.separatorChar + flowGroup + File.separatorChar + flowName), String.valueOf(flowExecutionId)) + suffix;
      return new File(this.specProducerPath + File.separatorChar + fileName).exists();
    } catch (URISyntaxException e) {
      log.error("URISyntaxException occurred when retrieving job status for flow: {},{}", flowGroup, flowName, e);
    }
    return false;
  }

  @Override
  public Iterator<JobStatus> getJobStatusesForFlowExecution(String flowName, String flowGroup, long flowExecutionId) {
    Preconditions.checkArgument(flowName != null, "FlowName cannot be null");
    Preconditions.checkArgument(flowGroup != null, "FlowGroup cannot be null");

    // For the FS use case, JobExecutionID == FlowExecutionID
    return getJobStatusesForFlowExecution(flowName, flowGroup, flowExecutionId, flowName, flowGroup);
  }

  @Override
  public Iterator<JobStatus> getJobStatusesForFlowExecution(String flowName, String flowGroup, long flowExecutionId,
      String jobName, String jobGroup) {
    Preconditions.checkArgument(flowName != null, "flowName cannot be null");
    Preconditions.checkArgument(flowGroup != null, "flowGroup cannot be null");
    Preconditions.checkArgument(jobName != null, "jobName cannot be null");
    Preconditions.checkArgument(jobGroup != null, "jobGroup cannot be null");
    List<JobStatus> jobStatuses = new ArrayList<>();
    JobStatus jobStatus;

    String JOB_DONE_SUFFIX = ".done";
    if (this.doesJobExist(flowName, flowGroup, flowExecutionId, JOB_DONE_SUFFIX)) {
      jobStatus = JobStatus.builder().flowName(flowName).flowGroup(flowGroup).flowExecutionId(flowExecutionId).
          jobName(jobName).jobGroup(jobGroup).jobExecutionId(flowExecutionId).eventName(ExecutionStatus.COMPLETE.name()).build();
    } else if (this.doesJobExist(flowName, flowGroup, flowExecutionId, "")) {
      jobStatus = JobStatus.builder().flowName(flowName).flowGroup(flowGroup).flowExecutionId(flowExecutionId).
          jobName(jobName).jobGroup(jobGroup).jobExecutionId(flowExecutionId).eventName(ExecutionStatus.PENDING.name()).build();
    } else {
      return Iterators.emptyIterator();
    }

    jobStatuses.add(jobStatus);
    return jobStatuses.iterator();
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

    //TODO: implement this

    return null;
  }

  /**
   * @param flowGroup
   * @return the last <code>countJobStatusesPerFlowName</code> flow statuses within the given flowGroup.
   */
  @Override
  public List<FlowStatus> getFlowStatusesForFlowGroupExecutions(String flowGroup, int countJobStatusesPerFlowName) {
    Preconditions.checkArgument(flowGroup != null, "flowGroup cannot be null");
    Preconditions.checkArgument(countJobStatusesPerFlowName > 0,
        "Number of job statuses per flow name must be at least 1 (was: %s).", countJobStatusesPerFlowName);
    throw new UnsupportedOperationException("Not yet implemented");
  }

  public StateStore<State> getStateStore() {
    // this jobstatus retriever does not have a state store
    // only used in tests so this is okay
    return null;
  }
}
