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

import java.util.Map;

import com.google.common.collect.Maps;
import com.typesafe.config.Config;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.runtime.api.SpecExecutor;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.util.ConfigUtils;


class TimingEventUtils {
  static Map<String, String> getFlowMetadata(FlowSpec flowSpec) {
    return getFlowMetadata(flowSpec.getConfig());
  }

  static Map<String, String> getFlowMetadata(Config flowConfig) {
    Map<String, String> metadata = Maps.newHashMap();

    metadata.put(TimingEvent.FlowEventConstants.FLOW_NAME_FIELD, flowConfig.getString(ConfigurationKeys.FLOW_NAME_KEY));
    metadata.put(TimingEvent.FlowEventConstants.FLOW_GROUP_FIELD, flowConfig.getString(ConfigurationKeys.FLOW_GROUP_KEY));

    if (flowConfig.hasPath(ConfigurationKeys.FLOW_EXECUTION_ID_KEY)) {
      metadata.put(TimingEvent.FlowEventConstants.FLOW_EXECUTION_ID_FIELD, flowConfig.getString(ConfigurationKeys.FLOW_EXECUTION_ID_KEY));
    }
    return metadata;
  }

  static Map<String, String> getJobMetadata(Map<String, String> flowMetadata, JobExecutionPlan jobExecutionPlan) {
    Map<String, String> jobMetadata = Maps.newHashMap();
    JobSpec jobSpec = jobExecutionPlan.getJobSpec();
    SpecExecutor specExecutor = jobExecutionPlan.getSpecExecutor();

    jobMetadata.putAll(flowMetadata);
    jobMetadata.put(TimingEvent.FlowEventConstants.FLOW_NAME_FIELD, jobSpec.getConfig().getString(ConfigurationKeys.FLOW_NAME_KEY));
    jobMetadata.put(TimingEvent.FlowEventConstants.FLOW_GROUP_FIELD, jobSpec.getConfig().getString(ConfigurationKeys.FLOW_GROUP_KEY));
    jobMetadata.put(TimingEvent.FlowEventConstants.FLOW_EXECUTION_ID_FIELD, jobSpec.getConfig().getString(ConfigurationKeys.FLOW_EXECUTION_ID_KEY));
    jobMetadata.put(TimingEvent.FlowEventConstants.JOB_NAME_FIELD, jobSpec.getConfig().getString(ConfigurationKeys.JOB_NAME_KEY));
    jobMetadata.put(TimingEvent.FlowEventConstants.JOB_GROUP_FIELD, jobSpec.getConfig().getString(ConfigurationKeys.JOB_GROUP_KEY));
    jobMetadata.put(TimingEvent.FlowEventConstants.JOB_TAG_FIELD, ConfigUtils.getString(jobSpec.getConfig(), ConfigurationKeys.JOB_TAG_KEY, null));
    jobMetadata.put(TimingEvent.FlowEventConstants.SPEC_EXECUTOR_FIELD, specExecutor.getClass().getCanonicalName());
    jobMetadata.put(TimingEvent.FlowEventConstants.MAX_ATTEMPTS_FIELD, Integer.toString(jobExecutionPlan.getMaxAttempts()));
    jobMetadata.put(TimingEvent.FlowEventConstants.CURRENT_ATTEMPTS_FIELD, Integer.toString(jobExecutionPlan.getCurrentAttempts()));
    jobMetadata.put(TimingEvent.FlowEventConstants.CURRENT_GENERATION_FIELD, Integer.toString(jobExecutionPlan.getCurrentGeneration()));
    jobMetadata.put(TimingEvent.FlowEventConstants.SHOULD_RETRY_FIELD, Boolean.toString(false));

    return jobMetadata;
  }

}
