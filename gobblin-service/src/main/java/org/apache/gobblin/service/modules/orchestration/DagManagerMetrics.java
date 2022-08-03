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

package org.apache.gobblin.service.modules.orchestration;

import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metrics.ContextAwareCounter;
import org.apache.gobblin.metrics.ContextAwareGauge;
import org.apache.gobblin.metrics.ContextAwareMeter;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.RootMetricContext;
import org.apache.gobblin.metrics.ServiceMetricNames;
import org.apache.gobblin.metrics.metric.filter.MetricNameRegexFilter;
import org.apache.gobblin.service.FlowId;
import org.apache.gobblin.service.RequesterService;
import org.apache.gobblin.service.ServiceRequester;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.util.ConfigUtils;


@Slf4j
public class DagManagerMetrics {
  private static final Map<String, DagManager.FlowState> flowGauges = Maps.newConcurrentMap();
  // Meters representing the total number of flows in a given state
  private ContextAwareMeter allSuccessfulMeter;
  private ContextAwareMeter allFailedMeter;
  private ContextAwareMeter allRunningMeter;
  private ContextAwareMeter allSlaExceededMeter;
  private ContextAwareMeter allStartSlaExceededMeter;
  // Meters representing the flows in a given state per flowgroup
  private final Map<String, ContextAwareMeter> groupSuccessfulMeters = Maps.newConcurrentMap();
  private final Map<String, ContextAwareMeter> groupFailureMeters = Maps.newConcurrentMap();
  private final Map<String, ContextAwareMeter> groupStartSlaExceededMeters = Maps.newConcurrentMap();
  private final Map<String, ContextAwareMeter> groupSlaExceededMeters = Maps.newConcurrentMap();

  // Meters representing the jobs in a given state per executor
  // These metrics need to be invoked differently to account for automated retries and multihop scenarios.
  private final Map<String, ContextAwareMeter> executorSuccessMeters = Maps.newConcurrentMap();
  private final Map<String, ContextAwareMeter> executorFailureMeters = Maps.newConcurrentMap();
  private final Map<String, ContextAwareMeter> executorStartSlaExceededMeters = Maps.newConcurrentMap();
  private final Map<String, ContextAwareMeter> executorSlaExceededMeters = Maps.newConcurrentMap();
  private final Map<String, ContextAwareMeter> executorJobSentMeters = Maps.newConcurrentMap();
  MetricContext metricContext;

  public DagManagerMetrics(MetricContext metricContext) {
    this.metricContext = metricContext;
  }

  public void activate() {
    if (this.metricContext != null) {
      allSuccessfulMeter = metricContext.contextAwareMeter(MetricRegistry.name(ServiceMetricNames.GOBBLIN_SERVICE_PREFIX,
          ServiceMetricNames.SUCCESSFUL_FLOW_METER));
      allFailedMeter = metricContext.contextAwareMeter(MetricRegistry.name(ServiceMetricNames.GOBBLIN_SERVICE_PREFIX,
          ServiceMetricNames.FAILED_FLOW_METER));
      allStartSlaExceededMeter = metricContext.contextAwareMeter(MetricRegistry.name(ServiceMetricNames.GOBBLIN_SERVICE_PREFIX,
          ServiceMetricNames.START_SLA_EXCEEDED_FLOWS_METER));
      allSlaExceededMeter = metricContext.contextAwareMeter(MetricRegistry.name(ServiceMetricNames.GOBBLIN_SERVICE_PREFIX,
          ServiceMetricNames.SLA_EXCEEDED_FLOWS_METER));
      allRunningMeter = metricContext.contextAwareMeter(MetricRegistry.name(ServiceMetricNames.GOBBLIN_SERVICE_PREFIX,
          ServiceMetricNames.JOBS_SENT_TO_SPEC_EXECUTOR));
    }
  }

  public void registerFlowMetric(FlowId flowId, Dag<JobExecutionPlan> dag) {
    // Do not register flow-specific metrics for an adhoc flow
    if (!flowGauges.containsKey(flowId.toString()) && DagManagerUtils.shouldFlowOutputMetrics(dag)) {
      String flowStateGaugeName = MetricRegistry.name(ServiceMetricNames.GOBBLIN_SERVICE_PREFIX, flowId.getFlowGroup(),
          flowId.getFlowName(), ServiceMetricNames.RUNNING_STATUS);
      flowGauges.put(flowId.toString(), DagManager.FlowState.RUNNING);
      ContextAwareGauge<Integer> gauge = RootMetricContext
          .get().newContextAwareGauge(flowStateGaugeName, () -> flowGauges.get(flowId.toString()).value);
      RootMetricContext.get().register(flowStateGaugeName, gauge);
    }
  }

  public void incrementRunningJobMetrics(Dag.DagNode<JobExecutionPlan> dagNode) {
    if (this.metricContext != null) {
      this.getRunningJobsCounterForExecutor(dagNode).inc();
      this.getRunningJobsCounterForUser(dagNode).forEach(ContextAwareCounter::inc);
    }
  }

  public void decrementRunningJobMetrics(Dag.DagNode<JobExecutionPlan> dagNode) {
    if (this.metricContext != null) {
      this.getRunningJobsCounterForExecutor(dagNode).dec();
      this.getRunningJobsCounterForUser(dagNode).forEach(ContextAwareCounter::dec);
    }
  }

  /**
   * Updates flowGauges with the appropriate state if the gauge is being tracked for the flow
   * @param flowId
   * @param state
   */
  public void conditionallyMarkFlowAsState(FlowId flowId, DagManager.FlowState state) {
    if (flowGauges.containsKey(flowId.toString())) {
      flowGauges.put(flowId.toString(), state);
    }
  }

  public void emitFlowSuccessMetrics(FlowId flowId) {
    if (this.metricContext != null) {
      this.conditionallyMarkFlowAsState(flowId, DagManager.FlowState.SUCCESSFUL);
      this.allSuccessfulMeter.mark();
      this.getGroupMeterForDag(flowId.getFlowGroup(), ServiceMetricNames.SUCCESSFUL_FLOW_METER, groupSuccessfulMeters).mark();
    }
  }

  public void emitFlowFailedMetrics(FlowId flowId) {
    if (this.metricContext != null) {
      this.conditionallyMarkFlowAsState(flowId, DagManager.FlowState.FAILED);
      this.allFailedMeter.mark();
      this.getGroupMeterForDag(flowId.getFlowGroup(), ServiceMetricNames.FAILED_FLOW_METER, groupFailureMeters).mark();
    }
  }

  public void emitFlowSlaExceededMetrics(FlowId flowId) {
    if (this.metricContext != null) {
      this.conditionallyMarkFlowAsState(flowId, DagManager.FlowState.FAILED);
      this.allSlaExceededMeter.mark();
      this.getGroupMeterForDag(flowId.getFlowGroup(), ServiceMetricNames.SLA_EXCEEDED_FLOWS_METER, groupSlaExceededMeters).mark();
    }
  }

  public void incrementExecutorSuccess(Dag.DagNode<JobExecutionPlan> node) {
    if (this.metricContext != null) {
      this.getExecutorMeterForDag(node, ServiceMetricNames.SUCCESSFUL_FLOW_METER, executorSuccessMeters).mark();
    }
  }

  public void incrementExecutorFailed(Dag.DagNode<JobExecutionPlan> node) {
    if (this.metricContext != null) {
      this.getExecutorMeterForDag(node, ServiceMetricNames.FAILED_FLOW_METER, executorFailureMeters).mark();
    }
  }

  public void incrementExecutorSlaExceeded(Dag.DagNode<JobExecutionPlan> node) {
    if (this.metricContext != null) {
      this.getExecutorMeterForDag(node, ServiceMetricNames.SLA_EXCEEDED_FLOWS_METER, executorSlaExceededMeters).mark();
    }
  }

  public void incrementJobsSentToExecutor(Dag.DagNode<JobExecutionPlan> node) {
    if (this.metricContext != null) {
      this.getExecutorMeterForDag(node, ServiceMetricNames.JOBS_SENT_TO_SPEC_EXECUTOR, executorJobSentMeters).mark();
      this.allRunningMeter.mark();
    }
  }

  // Increment the counts for start sla during the flow submission rather than cleanup to account for retries obfuscating this metric
  public void incrementCountsStartSlaExceeded(Dag.DagNode<JobExecutionPlan> node) {
    String flowGroup = node.getValue().getJobSpec().getConfig().getString(ConfigurationKeys.FLOW_GROUP_KEY);
    if (this.metricContext != null) {
      this.getGroupMeterForDag(flowGroup, ServiceMetricNames.START_SLA_EXCEEDED_FLOWS_METER, groupStartSlaExceededMeters);
      this.allStartSlaExceededMeter.mark();
      this.getExecutorMeterForDag(node, ServiceMetricNames.START_SLA_EXCEEDED_FLOWS_METER, executorStartSlaExceededMeters).mark();
    }
  }

  private List<ContextAwareCounter> getRunningJobsCounterForUser(Dag.DagNode<JobExecutionPlan> dagNode) {
    Config configs = dagNode.getValue().getJobSpec().getConfig();
    String proxy = ConfigUtils.getString(configs, AzkabanProjectConfig.USER_TO_PROXY, null);
    List<ContextAwareCounter> counters = new ArrayList<>();

    if (StringUtils.isNotEmpty(proxy)) {
      counters.add(this.metricContext.contextAwareCounter(
          MetricRegistry.name(
              ServiceMetricNames.GOBBLIN_SERVICE_PREFIX,
              ServiceMetricNames.SERVICE_USERS, proxy)));
    }

    try {
      String serializedRequesters = DagManagerUtils.getSerializedRequesterList(dagNode);
      if (StringUtils.isNotEmpty(serializedRequesters)) {
        List<ServiceRequester> requesters = RequesterService.deserialize(serializedRequesters);
        for (ServiceRequester requester : requesters) {
          counters.add(this.metricContext.contextAwareCounter(MetricRegistry
              .name(ServiceMetricNames.GOBBLIN_SERVICE_PREFIX, ServiceMetricNames.SERVICE_USERS, requester.getName())));
        }
      }
    } catch (IOException e) {
      log.error("Error while fetching requester list.", e);
    }

    return counters;
  }

  private ContextAwareCounter getRunningJobsCounterForExecutor(Dag.DagNode<JobExecutionPlan> dagNode) {
    return this.metricContext.contextAwareCounter(
        MetricRegistry.name(
            ServiceMetricNames.GOBBLIN_SERVICE_PREFIX,
            DagManagerUtils.getSpecExecutorName(dagNode),
            ServiceMetricNames.RUNNING_FLOWS_COUNTER));
  }


  private ContextAwareMeter getGroupMeterForDag(String flowGroup, String meterName, Map<String, ContextAwareMeter> meterMap) {
    return meterMap.computeIfAbsent(flowGroup,
        group -> metricContext.contextAwareMeter(MetricRegistry.name(ServiceMetricNames.GOBBLIN_SERVICE_PREFIX, group, meterName)));
  }

  /**
   * Used to track metrics for different specExecutors to detect issues with the specExecutor itself
   * @param dagNode
   * @param meterName
   * @param meterMap
   * @return
   */
  private ContextAwareMeter getExecutorMeterForDag(Dag.DagNode<JobExecutionPlan> dagNode, String meterName, Map<String, ContextAwareMeter> meterMap) {
    String executorName = DagManagerUtils.getSpecExecutorName(dagNode);
    return meterMap.computeIfAbsent(executorName,
        executorUri -> metricContext.contextAwareMeter(MetricRegistry.name(ServiceMetricNames.GOBBLIN_SERVICE_PREFIX, executorUri, meterName)));
  }


  @VisibleForTesting
  protected static MetricNameRegexFilter getMetricsFilterForDagManager() {
    return new MetricNameRegexFilter(ServiceMetricNames.GOBBLIN_SERVICE_PREFIX + "\\..*\\." + ServiceMetricNames.RUNNING_STATUS);
  }

  public void cleanup() {
    // The DMThread's metrics mappings follow the lifecycle of the DMThread itself and so are lost by DM deactivation-reactivation but the RootMetricContext is a (persistent) singleton.
    // To avoid IllegalArgumentException by the RMC preventing (re-)add of a metric already known, remove all metrics that a new DMThread thread would attempt to add (in DagManagerThread::initialize) whenever running post-re-enablement
    RootMetricContext.get().removeMatching(getMetricsFilterForDagManager());
  }
}
