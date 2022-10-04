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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigValueFactory;

import lombok.Data;
import lombok.EqualsAndHashCode;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.DynamicConfigGenerator;
import org.apache.gobblin.kafka.schemareg.KafkaSchemaRegistryConfigurationKeys;
import org.apache.gobblin.metrics.kafka.KafkaSchemaRegistry;
import org.apache.gobblin.runtime.DynamicConfigGeneratorFactory;
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.runtime.api.SpecExecutor;
import org.apache.gobblin.service.ExecutionStatus;
import org.apache.gobblin.service.modules.flowgraph.DatasetDescriptorConfigKeys;
import org.apache.gobblin.service.modules.flowgraph.FlowGraphConfigurationKeys;
import org.apache.gobblin.service.modules.orchestration.DagManager;
import org.apache.gobblin.util.ConfigUtils;

import static org.apache.gobblin.runtime.AbstractJobLauncher.GOBBLIN_JOB_TEMPLATE_KEY;


/**
 * A data class that encapsulates information for executing a job. This includes a {@link JobSpec} and a {@link SpecExecutor}
 * where the {@link JobSpec} will be executed.
 */
@Data
@EqualsAndHashCode(exclude = {"executionStatus", "currentAttempts", "jobFuture", "flowStartTime"})
public class JobExecutionPlan {
  public static final String JOB_MAX_ATTEMPTS = "job.maxAttempts";
  private static final int MAX_JOB_NAME_LENGTH = 255;

  private final JobSpec jobSpec;
  private final SpecExecutor specExecutor;
  private ExecutionStatus executionStatus = ExecutionStatus.PENDING;
  private final int maxAttempts;
  private int currentGeneration = 1;
  private int currentAttempts = 0;
  private Optional<Future> jobFuture = Optional.absent();
  private long flowStartTime = 0L;

  public static class Factory {
    public static final String JOB_NAME_COMPONENT_SEPARATION_CHAR = "_";

    public JobExecutionPlan createPlan(FlowSpec flowSpec, Config jobConfig, SpecExecutor specExecutor, Long flowExecutionId, Config sysConfig)
        throws URISyntaxException {
        try {
          JobSpec jobSpec = buildJobSpec(flowSpec, jobConfig, flowExecutionId, sysConfig, specExecutor.getConfig().get());
          return new JobExecutionPlan(jobSpec, specExecutor);
        } catch (InterruptedException | ExecutionException e) {
          throw new RuntimeException(e);
        }
    }

    /**
     * Given a resolved job config, this helper method converts the config to a {@link JobSpec}.
     * @param flowSpec input FlowSpec.
     * @param jobConfig resolved job config.
     * @param flowExecutionId flow execution id for the flow
     * @param sysConfig gobblin service level configs
     * @param specExecutorConfig configs for the {@link SpecExecutor} of this {@link JobExecutionPlan}
     * @return a {@link JobSpec} corresponding to the resolved job config.
     * @throws URISyntaxException if creation of {@link JobSpec} URI fails
     */
    private static JobSpec buildJobSpec(FlowSpec flowSpec, Config jobConfig, Long flowExecutionId, Config sysConfig, Config specExecutorConfig)
        throws URISyntaxException {
      Config flowConfig = flowSpec.getConfig();

      String flowName = ConfigUtils.getString(flowConfig, ConfigurationKeys.FLOW_NAME_KEY, "");
      String flowGroup = ConfigUtils.getString(flowConfig, ConfigurationKeys.FLOW_GROUP_KEY, "");
      String flowFailureOption = ConfigUtils.getString(flowConfig, ConfigurationKeys.FLOW_FAILURE_OPTION, DagManager.DEFAULT_FLOW_FAILURE_OPTION);
      String flowInputPath = ConfigUtils.getString(flowConfig, DatasetDescriptorConfigKeys.FLOW_INPUT_DATASET_DESCRIPTOR_PREFIX
          + "." + DatasetDescriptorConfigKeys.PATH_KEY, "");

      String jobName = ConfigUtils.getString(jobConfig, ConfigurationKeys.JOB_NAME_KEY, "");
      String edgeId = ConfigUtils.getString(jobConfig, FlowGraphConfigurationKeys.FLOW_EDGE_ID_KEY, "");

      // Modify the job name to include the flow group, flow name, edge id, and a random string to avoid collisions since
      // job names are assumed to be unique within a dag.
      int hash = flowInputPath.hashCode();
      jobName = Joiner.on(JOB_NAME_COMPONENT_SEPARATION_CHAR).join(flowGroup, flowName, jobName, edgeId, hash);
      // jobNames are commonly used as a directory name, which is limited to 255 characters
      if (jobName.length() >= MAX_JOB_NAME_LENGTH) {
        // shorten job length to be 128 characters (flowGroup) + (hashed) flowName, hashCode length
        jobName = Joiner.on(JOB_NAME_COMPONENT_SEPARATION_CHAR).join(flowGroup, flowName.hashCode(), hash);
      }
      JobSpec.Builder jobSpecBuilder = JobSpec.builder(jobSpecURIGenerator(flowGroup, jobName, flowSpec)).withConfig(jobConfig)
          .withDescription(flowSpec.getDescription()).withVersion(flowSpec.getVersion());

      //Get job template uri
      URI jobTemplateUri = new URI(jobConfig.getString(ConfigurationKeys.JOB_TEMPLATE_PATH));
      JobSpec jobSpec = jobSpecBuilder.withTemplate(jobTemplateUri).build();

      jobSpec.setConfig(jobSpec.getConfig()
          //Add flowGroup to job spec
          .withValue(ConfigurationKeys.FLOW_GROUP_KEY, ConfigValueFactory.fromAnyRef(flowGroup))
          //Add flowName to job spec
          .withValue(ConfigurationKeys.FLOW_NAME_KEY, ConfigValueFactory.fromAnyRef(flowName))
          //Add flow execution id
          .withValue(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, ConfigValueFactory.fromAnyRef(flowExecutionId))
          // Remove schedule due to namespace conflict with azkaban schedule key, but still keep track if flow is scheduled or not
          .withValue(ConfigurationKeys.GOBBLIN_OUTPUT_JOB_LEVEL_METRICS, ConfigValueFactory.fromAnyRef(jobSpec.getConfig().hasPath(ConfigurationKeys.JOB_SCHEDULE_KEY)))
          .withoutPath(ConfigurationKeys.JOB_SCHEDULE_KEY)
          //Remove template uri
          .withoutPath(GOBBLIN_JOB_TEMPLATE_KEY)
          // Add job.name and job.group
          .withValue(ConfigurationKeys.JOB_NAME_KEY, ConfigValueFactory.fromAnyRef(jobName))
          .withValue(ConfigurationKeys.JOB_GROUP_KEY, ConfigValueFactory.fromAnyRef(flowGroup))
          //Add flow failure option
          .withValue(ConfigurationKeys.FLOW_FAILURE_OPTION, ConfigValueFactory.fromAnyRef(flowFailureOption))
      );

      //Add tracking config to JobSpec.
      addTrackingEventConfig(jobSpec, sysConfig);

      addAdditionalConfig(jobSpec, sysConfig, specExecutorConfig);

      // Add dynamic config to jobSpec if a dynamic config generator is specified in sysConfig
      DynamicConfigGenerator dynamicConfigGenerator = DynamicConfigGeneratorFactory.createDynamicConfigGenerator(sysConfig);
      Config dynamicConfig = dynamicConfigGenerator.generateDynamicConfig(jobSpec.getConfig().withFallback(sysConfig));
      jobSpec.setConfig(dynamicConfig.withFallback(jobSpec.getConfig()));

      // Reset properties in Spec from Config
      jobSpec.setConfigAsProperties(ConfigUtils.configToProperties(jobSpec.getConfig()));

      return jobSpec;
    }

    /**
     * A method to add any additional configurations to a JobSpec which need to be passed to the {@link SpecExecutor}.
     * This enables {@link org.apache.gobblin.metrics.GobblinTrackingEvent}s to be emitted from each Gobblin job
     * orchestrated by Gobblin-as-a-Service, which will then be used for tracking the execution status of the job.
     * @param jobSpec representing a fully resolved {@link JobSpec}.
     * @param sysConfig gobblin service level configs
     * @param specExecutorConfig configs for the {@link SpecExecutor} of this {@link JobExecutionPlan}
     */
    private static void addAdditionalConfig(JobSpec jobSpec, Config sysConfig, Config specExecutorConfig) {
      if (!(sysConfig.hasPath(ConfigurationKeys.SPECEXECUTOR_CONFIGS_PREFIX_KEY)
          && !Strings.isNullOrEmpty(ConfigUtils.getString(sysConfig, ConfigurationKeys.SPECEXECUTOR_CONFIGS_PREFIX_KEY, ""))
          && sysConfig.hasPath(sysConfig.getString(ConfigurationKeys.SPECEXECUTOR_CONFIGS_PREFIX_KEY)))) {
        return;
      }

      String additionalConfigsPrefix = sysConfig.getString(ConfigurationKeys.SPECEXECUTOR_CONFIGS_PREFIX_KEY);

      Config config = jobSpec.getConfig().withFallback(ConfigUtils.getConfigOrEmpty(sysConfig, additionalConfigsPrefix));

      config = config.withFallback(ConfigUtils.getConfigOrEmpty(specExecutorConfig, additionalConfigsPrefix));

      if (!config.isEmpty()) {
        jobSpec.setConfig(config);
      }
    }

    /**
     * A method to add tracking event configurations to a JobSpec.
     * This enables {@link org.apache.gobblin.metrics.GobblinTrackingEvent}s
     * to be emitted from each Gobblin job orchestrated by Gobblin-as-a-Service, which will then be used for tracking the
     * execution status of the job.
     * @param jobSpec representing a fully resolved {@link JobSpec}.
     */
    private static void addTrackingEventConfig(JobSpec jobSpec, Config sysConfig) {
      Config reportingConfig = ConfigUtils.getConfig(sysConfig, ConfigurationKeys.METRICS_REPORTING_CONFIGURATIONS_PREFIX, ConfigFactory.empty());
      if (!reportingConfig.isEmpty()) {
        Config jobConfig = jobSpec.getConfig().withFallback(reportingConfig.atPath(ConfigurationKeys.METRICS_REPORTING_CONFIGURATIONS_PREFIX));
        boolean isSchemaRegistryEnabled = ConfigUtils.getBoolean(sysConfig, ConfigurationKeys.METRICS_REPORTING_KAFKA_USE_SCHEMA_REGISTRY, false);
        if (isSchemaRegistryEnabled) {
          String schemaRegistryUrl = ConfigUtils.getString(sysConfig, KafkaSchemaRegistry.KAFKA_SCHEMA_REGISTRY_URL, "");
          if (!Strings.isNullOrEmpty(schemaRegistryUrl)) {
            jobConfig = jobConfig.withValue(KafkaSchemaRegistry.KAFKA_SCHEMA_REGISTRY_URL, ConfigValueFactory.fromAnyRef(schemaRegistryUrl));
          }
          String schemaOverrideNamespace = ConfigUtils
              .getString(sysConfig, KafkaSchemaRegistryConfigurationKeys.KAFKA_SCHEMA_REGISTRY_OVERRIDE_NAMESPACE, "");
          if (!Strings.isNullOrEmpty(schemaOverrideNamespace)) {
            jobConfig = jobConfig.withValue(KafkaSchemaRegistryConfigurationKeys.KAFKA_SCHEMA_REGISTRY_OVERRIDE_NAMESPACE,
                ConfigValueFactory.fromAnyRef(schemaOverrideNamespace));
          }
        }
        jobSpec.setConfig(jobConfig);
      }
    }

    /**
     * A naive implementation of generating a jobSpec's URI within a multi-hop flow that follows the convention:
     * <JOB_CATALOG_SCHEME>/{@link ConfigurationKeys#JOB_GROUP_KEY}/{@link ConfigurationKeys#JOB_NAME_KEY}.
     */
    private static URI jobSpecURIGenerator(String jobGroup, String jobName, FlowSpec flowSpec)
        throws URISyntaxException {
      return new URI(JobSpec.Builder.DEFAULT_JOB_CATALOG_SCHEME, flowSpec.getUri().getAuthority(),
          StringUtils.appendIfMissing(StringUtils.prependIfMissing(flowSpec.getUri().getPath(), "/"), "/") + jobGroup
              + "/" + jobName, null);
    }
  }

  public JobExecutionPlan(JobSpec jobSpec, SpecExecutor specExecutor) {
    this.jobSpec = jobSpec;
    this.specExecutor = specExecutor;
    this.maxAttempts = ConfigUtils.getInt(jobSpec.getConfig(), JOB_MAX_ATTEMPTS, 1);
  }

  /**
   * Render the JobSpec into a JSON string.
   * @return a valid JSON string representation of the JobSpec.
   */
  @Override
  public String toString() {
    return jobSpec.getConfig().root().render(ConfigRenderOptions.concise());
  }
}
