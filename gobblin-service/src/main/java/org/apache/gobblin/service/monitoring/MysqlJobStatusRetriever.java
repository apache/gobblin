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

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.collect.Ordering;
import com.typesafe.config.Config;

import javax.inject.Inject;
import javax.inject.Singleton;
import lombok.Getter;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.metastore.MysqlJobStatusStateStore;
import org.apache.gobblin.metastore.MysqlJobStatusStateStoreFactory;
import org.apache.gobblin.metrics.ServiceMetricNames;
import org.apache.gobblin.runtime.troubleshooter.MultiContextIssueRepository;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.util.ConfigUtils;


/**
 * Mysql based Retriever for {@link JobStatus}.
 */
@Singleton
public class MysqlJobStatusRetriever extends JobStatusRetriever {

  @FunctionalInterface
  private interface SupplierThrowingIO<T> {
    T get() throws IOException;
  }

  public static final String MYSQL_JOB_STATUS_RETRIEVER_PREFIX = "mysqlJobStatusRetriever";
  public static final String GET_LATEST_JOB_STATUS_METRIC = MetricRegistry.name(ServiceMetricNames.GOBBLIN_SERVICE_PREFIX,
      MYSQL_JOB_STATUS_RETRIEVER_PREFIX, "getLatestJobStatus");
  public static final String GET_LATEST_FLOW_STATUS_METRIC = MetricRegistry.name(ServiceMetricNames.GOBBLIN_SERVICE_PREFIX,
      MYSQL_JOB_STATUS_RETRIEVER_PREFIX, "getLatestFlowStatus");
  public static final String GET_LATEST_FLOW_GROUP_STATUS_METRIC = MetricRegistry.name(ServiceMetricNames.GOBBLIN_SERVICE_PREFIX,
      MYSQL_JOB_STATUS_RETRIEVER_PREFIX, "getLatestFlowGroupStatus");
  public static final String GET_ALL_FLOW_STATUSES_METRIC = MetricRegistry.name(ServiceMetricNames.GOBBLIN_SERVICE_PREFIX,
      MYSQL_JOB_STATUS_RETRIEVER_PREFIX, "getAllFlowStatuses");

  @Getter
  private final MysqlJobStatusStateStore<State> stateStore;

  @Inject
  public MysqlJobStatusRetriever(Config config, MultiContextIssueRepository issueRepository) throws ReflectiveOperationException {
    super(ConfigUtils.getBoolean(config, ServiceConfigKeys.GOBBLIN_SERVICE_DAG_MANAGER_ENABLED_KEY,
        ServiceConfigKeys.DEFAULT_GOBBLIN_SERVICE_DAG_MANAGER_ENABLED), issueRepository);
    config = config.getConfig(MYSQL_JOB_STATUS_RETRIEVER_PREFIX).withFallback(config);
    this.stateStore = (MysqlJobStatusStateStoreFactory.class.newInstance()).createStateStore(config, State.class);
  }

  @Override
  public Iterator<JobStatus> getJobStatusesForFlowExecution(String flowName, String flowGroup, long flowExecutionId) {
    String storeName = KafkaJobStatusMonitor.jobStatusStoreName(flowGroup, flowName);
    List<State> jobStatusStates = timeOpAndWrapIOException(() -> this.stateStore.getAll(storeName, flowExecutionId),
        GET_LATEST_FLOW_STATUS_METRIC);
    return asJobStatuses(jobStatusStates);
  }

  @Override
  public Iterator<JobStatus> getJobStatusesForFlowExecution(String flowName, String flowGroup, long flowExecutionId,
      String jobName, String jobGroup) {
    String storeName = KafkaJobStatusMonitor.jobStatusStoreName(flowGroup, flowName);
    String tableName = KafkaJobStatusMonitor.jobStatusTableName(flowExecutionId, jobGroup, jobName);
    List<State> jobStatusStates = timeOpAndWrapIOException(() -> this.stateStore.getAll(storeName, tableName),
        GET_LATEST_JOB_STATUS_METRIC);
    return asJobStatuses(jobStatusStates);
  }

  @Override
  public List<FlowStatus> getFlowStatusesForFlowGroupExecutions(String flowGroup, int countJobStatusesPerFlowName) {
    String storeNamePrefix = KafkaJobStatusMonitor.jobStatusStoreName(flowGroup, "");
    // TODO: optimize as needed: returned `List<State>` may be large, since encompassing every execution of every flow (in group)!
    List<State> jobStatusStates = timeOpAndWrapIOException(() -> this.stateStore.getAllWithPrefix(storeNamePrefix),
        GET_LATEST_FLOW_GROUP_STATUS_METRIC);
    return asFlowStatuses(groupByFlowExecutionAndRetainLatest(flowGroup, jobStatusStates, countJobStatusesPerFlowName));
  }

  @Override
  public List<Long> getLatestExecutionIdsForFlow(String flowName, String flowGroup, int count) {
    String storeName = KafkaJobStatusMonitor.jobStatusStoreName(flowGroup, flowName);
    List<State> jobStatusStates = timeOpAndWrapIOException(() -> this.stateStore.getAll(storeName),
        GET_ALL_FLOW_STATUSES_METRIC);
    return getLatestExecutionIds(jobStatusStates, count);
  }

  private List<State> timeOpAndWrapIOException(SupplierThrowingIO<List<State>> states, String timerMetricName) {
    try (Timer.Context context = this.metricContext.contextAwareTimer(timerMetricName).time()) {
      return states.get();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private List<Long> getLatestExecutionIds(List<State> jobStatusStates, int count) {
    // `distinct()`, to avoid each flow execution ID replicating as many times as it has child jobs
    Iterator<Long> flowExecutionIds = jobStatusStates.stream().map(this::getFlowExecutionId).distinct().iterator();
    return Ordering.<Long>natural().greatestOf(flowExecutionIds, count);
  }
}
