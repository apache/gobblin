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

package org.apache.gobblin.temporal.ddm.launcher;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.fs.Path;

import com.google.common.eventbus.EventBus;
import com.typesafe.config.ConfigFactory;

import io.temporal.client.WorkflowOptions;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.broker.SharedResourcesBrokerFactory;
import org.apache.gobblin.broker.gobblin_scopes.GobblinScopeTypes;
import org.apache.gobblin.broker.iface.SharedResourcesBroker;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metrics.opentelemetry.GaaSOpenTelemetryMetrics;
import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.runtime.JobContext;
import org.apache.gobblin.runtime.JobLauncher;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.temporal.cluster.GobblinTemporalTaskRunner;
import org.apache.gobblin.temporal.ddm.activity.impl.EmitOTelMetricsImpl;
import org.apache.gobblin.temporal.ddm.work.ExecGobblinStats;
import org.apache.gobblin.temporal.ddm.work.assistance.Help;
import org.apache.gobblin.temporal.ddm.workflow.ExecuteGobblinWorkflow;
import org.apache.gobblin.temporal.joblauncher.GobblinTemporalJobLauncher;
import org.apache.gobblin.temporal.joblauncher.GobblinTemporalJobScheduler;
import org.apache.gobblin.temporal.workflows.metrics.EventSubmitterContext;
import org.apache.gobblin.temporal.ddm.util.TemporalWorkFlowUtils;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.JobLauncherUtils;
import org.apache.gobblin.util.PropertiesUtils;

import static org.apache.gobblin.metrics.opentelemetry.GaaSOpenTelemetryMetricsConstants.DimensionKeys.*;
import static org.apache.gobblin.metrics.opentelemetry.GaaSOpenTelemetryMetricsConstants.DimensionValues.*;


/**
 * A {@link JobLauncher} for the initial triggering of a Temporal workflow that executes a full Gobblin job workflow of:
 *   * Work Discovery (via an arbitrary and configurable {@link org.apache.gobblin.source.Source})
 *   * Work Fulfillment/Processing
 *   * Commit
 *
 *  see: {@link ExecuteGobblinWorkflow} *
 *
 * <p>
 *   This class is instantiated by the {@link GobblinTemporalJobScheduler#buildJobLauncher(Properties)} on every job submission to launch the Gobblin job.
 *   The actual task execution happens in the {@link GobblinTemporalTaskRunner}, usually in a different process.
 * </p>
 */
@Slf4j
public class ExecuteGobblinJobLauncher extends GobblinTemporalJobLauncher {

  public static final String WORKFLOW_ID_BASE = "ExecuteGobblin";

  public ExecuteGobblinJobLauncher(
      Properties jobProps,
      Path appWorkDir,
      List<? extends Tag<?>> metadataTags,
      ConcurrentHashMap<String, Boolean> runningMap,
      EventBus eventBus
  ) throws Exception {
    super(jobProps, appWorkDir, metadataTags, runningMap, eventBus);
  }

  @Override
  public void submitJob(List<WorkUnit> workunits) {
    try {
      Properties finalProps = adjustJobProperties(this.jobProps);
      // Initialize workflowId.
      this.workflowId = Help.qualifyNamePerExecWithFlowExecId(WORKFLOW_ID_BASE, ConfigFactory.parseProperties(finalProps));
      WorkflowOptions options = WorkflowOptions.newBuilder()
          .setTaskQueue(this.queueName)
          .setSearchAttributes(TemporalWorkFlowUtils.generateGaasSearchAttributes(finalProps))
          .setWorkflowId(this.workflowId)
          .build();
      ExecuteGobblinWorkflow workflow = this.client.newWorkflowStub(ExecuteGobblinWorkflow.class, options);

      Help.propagateGaaSFlowExecutionContext(finalProps);
      EventSubmitterContext eventSubmitterContext = new EventSubmitterContext.Builder(eventSubmitter)
          .withGaaSJobProps(finalProps)
          .build();

      Map<String, String> attributes = new HashMap<>();
      attributes.put(CURR_STATE, JOB_START);
      EmitOTelMetricsImpl emitOTelMetrics = new EmitOTelMetricsImpl();
      emitOTelMetrics.emitLongCounterMetric(GaaSOpenTelemetryMetrics.GAAS_JOB_STATUS, 1L, attributes, finalProps);

      long startTimeMillis = System.currentTimeMillis();
      ExecGobblinStats execGobblinStats = workflow.execute(finalProps, eventSubmitterContext);
      double timeTaken = (System.currentTimeMillis() - startTimeMillis) / 1000.0;
      log.info("FINISHED - ExecuteGobblinWorkflow.execute = {}", execGobblinStats);
      attributes.put(CURR_STATE, JOB_COMPLETE);
      emitOTelMetrics.emitLongCounterMetric(GaaSOpenTelemetryMetrics.GAAS_JOB_STATUS, 1L, attributes, finalProps);
      attributes.remove(CURR_STATE);
      attributes.put(STATE, JOB_COMPLETE);
      emitOTelMetrics.emitDoubleHistogramMetric(GaaSOpenTelemetryMetrics.GAAS_JOB_STATE_LATENCY, timeTaken, attributes, finalProps);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  // Generate properties such as Job ID, modifying task staging dirs and output dirs
  protected Properties adjustJobProperties(Properties inputJobProps) throws Exception {
    SharedResourcesBroker<GobblinScopeTypes> instanceBroker = SharedResourcesBrokerFactory.createDefaultTopLevelBroker(ConfigFactory.parseProperties(inputJobProps),
        GobblinScopeTypes.GLOBAL.defaultScopeInstance());
    Properties configOverridesProp = ConfigUtils.configToProperties(applyJobLauncherOverrides(ConfigUtils.propertiesToConfig(inputJobProps)));
    configOverridesProp.setProperty(ConfigurationKeys.JOB_ID_KEY, JobLauncherUtils.newJobId(JobState.getJobNameFromProps(configOverridesProp),
        PropertiesUtils.getPropAsLong(configOverridesProp, ConfigurationKeys.FLOW_EXECUTION_ID_KEY, System.currentTimeMillis())));
    JobContext jobContext = new JobContext(configOverridesProp, log, instanceBroker, null);
    return jobContext.getJobState().getProperties();
  }
}
