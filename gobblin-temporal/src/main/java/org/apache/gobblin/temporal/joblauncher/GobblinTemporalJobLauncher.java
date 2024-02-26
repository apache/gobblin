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

package org.apache.gobblin.temporal.joblauncher;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.eventbus.EventBus;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import io.temporal.client.WorkflowClient;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.workflow.Workflow;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.cluster.GobblinClusterConfigurationKeys;
import org.apache.gobblin.cluster.event.ClusterManagerShutdownRequest;
import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.runtime.JobLauncher;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.temporal.cluster.GobblinTemporalTaskRunner;
import org.apache.gobblin.temporal.GobblinTemporalConfigurationKeys;
import org.apache.gobblin.util.ConfigUtils;

import static org.apache.gobblin.temporal.GobblinTemporalConfigurationKeys.*;
import static org.apache.gobblin.temporal.workflows.client.TemporalWorkflowClientFactory.createClientInstance;
import static org.apache.gobblin.temporal.workflows.client.TemporalWorkflowClientFactory.createServiceInstance;

/**
 * An implementation of {@link JobLauncher} that launches a Gobblin job using the Temporal task framework.
 *
 * <p>
 *   Each {@link WorkUnit} of the job is persisted to the {@link FileSystem} of choice and the path to the file
 *   storing the serialized {@link WorkUnit} is passed to the Temporal task running the {@link WorkUnit} as a
 *   user-defined property {@link GobblinClusterConfigurationKeys#WORK_UNIT_FILE_PATH}. Upon startup, the gobblin
 *   task reads the property for the file path and de-serializes the {@link WorkUnit} from the file.
 * </p>
 *
 * <p>
 *   This class is instantiated by the {@link GobblinTemporalJobScheduler} on every job submission to launch the Gobblin job.
 *   The actual task execution happens in the {@link GobblinTemporalTaskRunner}, usually in a different process.
 * </p>
 */
@Alpha
public abstract class GobblinTemporalJobLauncher extends GobblinJobLauncher {
  private static final Logger log = Workflow.getLogger(GobblinTemporalJobLauncher.class);

  protected WorkflowServiceStubs workflowServiceStubs;
  protected WorkflowClient client;
  protected String queueName;

  public GobblinTemporalJobLauncher(Properties jobProps, Path appWorkDir,
                                    List<? extends Tag<?>> metadataTags, ConcurrentHashMap<String, Boolean> runningMap, EventBus eventBus)
          throws Exception {
    super(jobProps, appWorkDir, metadataTags, runningMap, eventBus);
    log.info("GobblinTemporalJobLauncher: appWorkDir {}; jobProps {}", appWorkDir, jobProps);

    String connectionUri = jobProps.getProperty(TEMPORAL_CONNECTION_STRING);
    this.workflowServiceStubs = createServiceInstance(connectionUri);

    String namespace = jobProps.getProperty(GOBBLIN_TEMPORAL_NAMESPACE, DEFAULT_GOBBLIN_TEMPORAL_NAMESPACE);
    this.client = createClientInstance(workflowServiceStubs, namespace);

    this.queueName = jobProps.getProperty(GOBBLIN_TEMPORAL_TASK_QUEUE, DEFAULT_GOBBLIN_TEMPORAL_TASK_QUEUE);

    startCancellationExecutor();
  }

  /** @return {@link Config} now featuring all overrides rooted at {@link GobblinTemporalConfigurationKeys#GOBBLIN_TEMPORAL_JOB_LAUNCHER_CONFIG_OVERRIDES} */
  protected Config applyJobLauncherOverrides(Config config) {
    Config configOverrides = ConfigUtils.getConfig(config,
        GobblinTemporalConfigurationKeys.GOBBLIN_TEMPORAL_JOB_LAUNCHER_CONFIG_OVERRIDES, ConfigFactory.empty());
    log.info("appying config overrides: {}", configOverrides);
    return configOverrides.withFallback(config);
  }

  @Override
  protected void handleLaunchFinalization() {
    // NOTE: This code only makes sense when there is 1 source / workflow being launched per application for Temporal. This is a stop-gap
    // for achieving batch job behavior. Given the current constraints of yarn applications requiring a static proxy user
    // during application creation, it is not possible to have multiple workflows running in the same application.
    // and so it makes sense to just kill the job after this is completed
    log.info("Requesting the AM to shutdown after the job {} completed", this.jobContext.getJobId());
    eventBus.post(new ClusterManagerShutdownRequest());
  }

  /**
   * Submit a job to run.
   */
  @Override
  abstract protected void submitJob(List<WorkUnit> workUnits) throws Exception;

  @Override
  protected void executeCancellation() {
    log.info("Cancel temporal workflow");
  }

  /** No-op: merely logs a warning, since not expected to be invoked */
  @Override
  protected void removeTasksFromCurrentJob(List<String> workUnitIdsToRemove) {
    log.warn("NOT IMPLEMENTED: Temporal removeTasksFromCurrentJob");
  }

  /** No-op: merely logs a warning, since not expected to be invoked */
  @Override
  protected void addTasksToCurrentJob(List<WorkUnit> workUnitsToAdd) {
    log.warn("NOT IMPLEMENTED: Temporal addTasksToCurrentJob");
  }
}
