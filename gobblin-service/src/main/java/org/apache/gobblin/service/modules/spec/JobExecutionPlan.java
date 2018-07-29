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

package org.apache.gobblin.service.modules.spec;

import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Joiner;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;

import lombok.AllArgsConstructor;
import lombok.Data;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.runtime.api.SpecExecutor;
import org.apache.gobblin.util.ConfigUtils;


/**
 * A data class that encapsulates information for executing a job. This includes a {@link JobSpec} and a {@link SpecExecutor}
 * where the {@link JobSpec} will be executed.
 */
@Data
@AllArgsConstructor
public class JobExecutionPlan {
  private JobSpec jobSpec;
  private SpecExecutor specExecutor;

  public static class Factory {

    public JobExecutionPlan createPlan(FlowSpec flowSpec, Config jobConfig, SpecExecutor specExecutor, Long flowExecutionId)
        throws URISyntaxException {
        JobSpec jobSpec = buildJobSpec(flowSpec, jobConfig, flowExecutionId);
        return new JobExecutionPlan(jobSpec, specExecutor);
    }

    /**
     * Given a resolved job config, this helper method converts the config to a {@link JobSpec}.
     * @param jobConfig resolved job config.
     * @param flowSpec input FlowSpec.
     * @return a {@link JobSpec} corresponding to the resolved job config.
     */
    private static JobSpec buildJobSpec(FlowSpec flowSpec, Config jobConfig, Long flowExecutionId) throws URISyntaxException {
      Config flowConfig = flowSpec.getConfig();

      String flowName = ConfigUtils.getString(flowConfig, ConfigurationKeys.FLOW_NAME_KEY, "");
      String flowGroup = ConfigUtils.getString(flowConfig, ConfigurationKeys.FLOW_GROUP_KEY, "");
      String jobName = ConfigUtils.getString(jobConfig, ConfigurationKeys.JOB_NAME_KEY, "");

      //Modify the job name to include the flow group:flow name.
      jobName = Joiner.on(":").join(flowGroup, flowName, jobName);

      JobSpec.Builder jobSpecBuilder = JobSpec.builder(jobSpecURIGenerator(flowGroup, jobName, flowSpec)).withConfig(jobConfig)
          .withDescription(flowSpec.getDescription()).withVersion(flowSpec.getVersion());

      //Get job template uri
      URI jobTemplateUri = new URI(jobConfig.getString(ConfigurationKeys.JOB_TEMPLATE_PATH));
      JobSpec jobSpec = jobSpecBuilder.withTemplate(jobTemplateUri).build();

      //Add flowName to job spec
      jobSpec.setConfig(jobSpec.getConfig().withValue(ConfigurationKeys.FLOW_NAME_KEY, ConfigValueFactory.fromAnyRef(flowName)));

      //Add job name
      jobSpec.setConfig(jobSpec.getConfig().withValue(ConfigurationKeys.JOB_NAME_KEY, ConfigValueFactory.fromAnyRef(jobName)));

      //Add flow execution id
      jobSpec.setConfig(jobSpec.getConfig().withValue(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, ConfigValueFactory.fromAnyRef(flowExecutionId)));

      // Remove schedule
      jobSpec.setConfig(jobSpec.getConfig().withoutPath(ConfigurationKeys.JOB_SCHEDULE_KEY));

      // Add job.name and job.group
      jobSpec.setConfig(jobSpec.getConfig().withValue(ConfigurationKeys.JOB_NAME_KEY, ConfigValueFactory.fromAnyRef(jobName)));
      jobSpec.setConfig(jobSpec.getConfig().withValue(ConfigurationKeys.JOB_GROUP_KEY, ConfigValueFactory.fromAnyRef(flowGroup)));

      //Enable job lock for each job to prevent concurrent executions.
      jobSpec.setConfig(jobSpec.getConfig().withValue(ConfigurationKeys.JOB_LOCK_ENABLED_KEY, ConfigValueFactory.fromAnyRef(true)));

      // Reset properties in Spec from Config
      jobSpec.setConfigAsProperties(ConfigUtils.configToProperties(jobSpec.getConfig()));

      return jobSpec;
    }


    /**
     * A naive implementation of generating a jobSpec's URI within a multi-hop flow that follows the convention:
     * <JOB_CATALOG_SCHEME>/{@link ConfigurationKeys#JOB_GROUP_KEY}/{@link ConfigurationKeys#JOB_NAME_KEY}.
     */
    public static URI jobSpecURIGenerator(String jobGroup, String jobName, FlowSpec flowSpec)
        throws URISyntaxException {
      return new URI(JobSpec.Builder.DEFAULT_JOB_CATALOG_SCHEME, flowSpec.getUri().getAuthority(),
          StringUtils.appendIfMissing(StringUtils.prependIfMissing(flowSpec.getUri().getPath(), "/"), "/") + jobGroup
              + "/" + jobName, null);
    }
  }
}
