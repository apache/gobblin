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

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;


import com.google.common.eventbus.EventBus;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import io.temporal.client.WorkflowOptions;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.hadoop.fs.Path;

import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.runtime.JobLauncher;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.temporal.cluster.GobblinTemporalTaskRunner;
import org.apache.gobblin.temporal.ddm.work.assistance.Help;
import org.apache.gobblin.temporal.ddm.workflow.ExecuteGobblinWorkflow;
import org.apache.gobblin.temporal.joblauncher.GobblinTemporalJobLauncher;
import org.apache.gobblin.temporal.joblauncher.GobblinTemporalJobScheduler;
import org.apache.gobblin.temporal.workflows.metrics.EventSubmitterContext;
import org.apache.gobblin.util.ConfigUtils;


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
      WorkflowOptions options = WorkflowOptions.newBuilder()
          .setTaskQueue(this.queueName)
          .setWorkflowId(Help.qualifyNamePerExecWithFlowExecId(WORKFLOW_ID_BASE, ConfigFactory.parseProperties(jobProps)))
          .build();
      ExecuteGobblinWorkflow workflow = this.client.newWorkflowStub(ExecuteGobblinWorkflow.class, options);

      Config jobConfigWithOverrides = applyJobLauncherOverrides(ConfigUtils.propertiesToConfig(this.jobProps));

      Help.propagateGaaSFlowExecutionContext(this.jobProps);
      EventSubmitterContext eventSubmitterContext = new EventSubmitterContext(
          addAdditionalMetadataTags(this.jobProps, this.eventSubmitter.getTags()),
          eventSubmitter.getNamespace()
      );
      int numWorkUnits = workflow.execute(ConfigUtils.configToProperties(jobConfigWithOverrides), eventSubmitterContext);
      log.info("FINISHED - ExecuteGobblinWorkflow.execute = {}", numWorkUnits);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static List<Tag<?>> addAdditionalMetadataTags(Properties jobProps, List<Tag<?>> additionalTags) {
    // TODO: Add temporal specific metadata tags
    List<Tag<?>> metadataTags = new ArrayList<>(additionalTags);
    if (jobProps.containsKey(ConfigurationKeys.FLOW_GROUP_KEY)) {
      metadataTags.add(new Tag<>(TimingEvent.FlowEventConstants.FLOW_GROUP_FIELD, jobProps.getProperty(ConfigurationKeys.FLOW_GROUP_KEY)));
      metadataTags.add(new Tag<>(TimingEvent.FlowEventConstants.FLOW_NAME_FIELD, jobProps.getProperty(ConfigurationKeys.FLOW_NAME_KEY)));
      metadataTags.add(new Tag<>(TimingEvent.FlowEventConstants.FLOW_EXECUTION_ID_FIELD, jobProps.getProperty(ConfigurationKeys.FLOW_EXECUTION_ID_KEY)));
    }

    if (jobProps.containsKey(ConfigurationKeys.JOB_CURRENT_ATTEMPTS)) {
      metadataTags.add(new Tag<>(TimingEvent.FlowEventConstants.CURRENT_ATTEMPTS_FIELD,
          jobProps.getProperty(ConfigurationKeys.JOB_CURRENT_ATTEMPTS, "1")));
      metadataTags.add(new Tag<>(TimingEvent.FlowEventConstants.CURRENT_GENERATION_FIELD,
          jobProps.getProperty(ConfigurationKeys.JOB_CURRENT_GENERATION, "1")));
      metadataTags.add(new Tag<>(TimingEvent.FlowEventConstants.SHOULD_RETRY_FIELD,
          "false"));
    }

    //Use azkaban.flow.execid as the jobExecutionId
    metadataTags.add(new Tag<>(TimingEvent.FlowEventConstants.JOB_EXECUTION_ID_FIELD, "0"));

    metadataTags.add(new Tag<>(TimingEvent.FlowEventConstants.JOB_GROUP_FIELD,
        jobProps.getProperty(ConfigurationKeys.JOB_GROUP_KEY, "")));
    metadataTags.add(new Tag<>(TimingEvent.FlowEventConstants.JOB_NAME_FIELD,
        jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY, "")));
    metadataTags.add(new Tag<>(TimingEvent.METADATA_MESSAGE, ""));

    metadataTags.add(new Tag<>(Help.USER_TO_PROXY_KEY, jobProps.getProperty(Help.USER_TO_PROXY_KEY, "")));

    return metadataTags;
  }
}
