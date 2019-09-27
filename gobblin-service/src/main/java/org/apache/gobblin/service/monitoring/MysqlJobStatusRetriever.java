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
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;
import java.util.stream.Collectors;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.typesafe.config.Config;

import lombok.Getter;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.metastore.MysqlJobStatusStateStore;
import org.apache.gobblin.metastore.MysqlJobStatusStateStoreFactory;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.service.ExecutionStatus;


/**
 * Mysql based Retriever for {@link JobStatus}.
 */
public class MysqlJobStatusRetriever extends JobStatusRetriever {

  public static final String CONF_PREFIX = "mysqlJobStatusRetriever";
  @Getter
  private MysqlJobStatusStateStore<State> stateStore;

  public MysqlJobStatusRetriever(Config config) throws ReflectiveOperationException {
    config = config.getConfig(CONF_PREFIX).withFallback(config);
    this.stateStore = (MysqlJobStatusStateStoreFactory.class.newInstance()).createStateStore(config, State.class);
  }

  @Override
  public Iterator<JobStatus> getJobStatusesForFlowExecution(String flowName, String flowGroup, long flowExecutionId) {
    String storeName = KafkaJobStatusMonitor.jobStatusStoreName(flowGroup, flowName);
    try {
      List<State> jobStatusStates = this.stateStore.getAll(storeName, flowExecutionId);
      return getJobStatuses(jobStatusStates);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Iterator<JobStatus> getJobStatusesForFlowExecution(String flowName, String flowGroup, long flowExecutionId,
      String jobName, String jobGroup) {
    String storeName = KafkaJobStatusMonitor.jobStatusStoreName(flowGroup, flowName);
    String tableName = KafkaJobStatusMonitor.jobStatusTableName(flowExecutionId, jobGroup, jobName);

    try {
      List<State> jobStatusStates = this.stateStore.getAll(storeName, tableName);
      return getJobStatuses(jobStatusStates);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<Long> getLatestExecutionIdsForFlow(String flowName, String flowGroup, int count) {
    String storeName = KafkaJobStatusMonitor.jobStatusStoreName(flowGroup, flowName);

    try {
      List<State> jobStatusStates = this.stateStore.getAll(storeName);
      List<Long> flowExecutionIds = jobStatusStates.stream()
          .map(state -> Long.parseLong(state.getProp(TimingEvent.FlowEventConstants.FLOW_EXECUTION_ID_FIELD)))
          .collect(Collectors.toList());
      return ImmutableList.copyOf(Iterables.limit(new TreeSet<>(flowExecutionIds).descendingSet(), count));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private Iterator<JobStatus> getJobStatuses(List<State> jobStatusStates) {
    return jobStatusStates.stream().map(this::getJobStatus).iterator();
  }
}
