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

package org.apache.gobblin.cluster;

import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.gobblin.broker.SharedResourcesBrokerFactory;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.TaskCreationException;
import org.apache.gobblin.runtime.TaskState;
import org.apache.gobblin.runtime.util.StateStores;
import org.apache.gobblin.source.workunit.MultiWorkUnit;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.util.Id;
import org.apache.gobblin.util.event.ContainerHealthCheckFailureEvent;
import org.apache.gobblin.util.eventbus.EventBusFactory;
import org.apache.gobblin.util.retry.RetryerFactory;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.helix.task.JobContext;
import org.apache.helix.task.Task;
import org.apache.helix.task.TaskCallbackContext;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskResult;
import org.slf4j.MDC;

import com.github.rholder.retry.Retryer;
import com.google.common.base.Throwables;
import com.google.common.eventbus.EventBus;
import com.google.common.io.Closer;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import lombok.extern.slf4j.Slf4j;

import static org.apache.gobblin.cluster.HelixTaskEventMetadataGenerator.HELIX_INSTANCE_KEY;
import static org.apache.gobblin.cluster.HelixTaskEventMetadataGenerator.HELIX_JOB_ID_KEY;
import static org.apache.gobblin.cluster.HelixTaskEventMetadataGenerator.HELIX_TASK_ID_KEY;


/**
 * An implementation of Helix's {@link org.apache.helix.task.Task} that wraps and runs one or more Gobblin
 * {@link org.apache.gobblin.runtime.Task}s.
 *
 * <p>
 *   Upon startup, a {@link GobblinHelixTask} reads the property
 *   {@link GobblinClusterConfigurationKeys#WORK_UNIT_FILE_PATH} for the path of the file storing a serialized
 *   {@link WorkUnit} on the {@link FileSystem} of choice and de-serializes the {@link WorkUnit}. Depending on
 *   if the serialized {@link WorkUnit} is a {@link MultiWorkUnit}, it then creates one or more Gobblin
 *   {@link org.apache.gobblin.runtime.Task}s to run the {@link WorkUnit}(s) (possibly wrapped in the {@link MultiWorkUnit})
 *   and waits for the Gobblin {@link org.apache.gobblin.runtime.Task}(s) to finish. Upon completion of the Gobblin
 *   {@link org.apache.gobblin.runtime.Task}(s), it persists the {@link TaskState} of each {@link org.apache.gobblin.runtime.Task} to
 *   a file that will be collected by the {@link GobblinHelixJobLauncher} later upon completion of the job.
 * </p>
 */
@Slf4j
public class GobblinHelixTask implements Task {

  private final TaskConfig taskConfig;
  private final String applicationName;
  private final String instanceName;

  private String jobName;
  private String jobId;
  private String helixJobId;
  private String jobKey;
  private String taskId;
  private Path workUnitFilePath;
  private GobblinHelixTaskMetrics taskMetrics;
  private SingleTask task;
  private String helixTaskId;
  private EventBus eventBus;

  public GobblinHelixTask(TaskRunnerSuiteBase.Builder builder,
                          TaskCallbackContext taskCallbackContext,
                          TaskAttemptBuilder taskAttemptBuilder,
                          StateStores stateStores,
                          GobblinHelixTaskMetrics taskMetrics,
                          TaskDriver taskDriver)
  {
    this.taskConfig = taskCallbackContext.getTaskConfig();
    this.helixJobId = taskCallbackContext.getJobConfig().getJobId();
    this.applicationName = builder.getApplicationName();
    this.instanceName = builder.getInstanceName();
    this.taskMetrics = taskMetrics;
    getInfoFromTaskConfig();

    Path jobStateFilePath = GobblinClusterUtils
        .getJobStateFilePath(stateStores.haveJobStateStore(),
                             builder.getAppWorkPath(),
                             this.jobId);

    Integer partitionNum = getPartitionForHelixTask(taskDriver);
    if (partitionNum == null) {
      throw new IllegalStateException(String.format("Task %s, job %s on instance %s has no partition assigned",
          this.helixTaskId, builder.getInstanceName(), this.helixJobId));
    }

    // Dynamic config is considered as part of JobState in SingleTask
    // Important to distinguish between dynamicConfig and Config
    final Config dynamicConfig = builder.getDynamicConfig()
        .withValue(GobblinClusterConfigurationKeys.TASK_RUNNER_HOST_NAME_KEY, ConfigValueFactory.fromAnyRef(builder.getHostName()))
        .withValue(GobblinClusterConfigurationKeys.CONTAINER_ID_KEY, ConfigValueFactory.fromAnyRef(builder.getContainerId()))
        .withValue(GobblinClusterConfigurationKeys.HELIX_INSTANCE_NAME_KEY, ConfigValueFactory.fromAnyRef(builder.getInstanceName()))
        .withValue(GobblinClusterConfigurationKeys.HELIX_JOB_ID_KEY, ConfigValueFactory.fromAnyRef(this.helixJobId))
        .withValue(GobblinClusterConfigurationKeys.HELIX_TASK_ID_KEY, ConfigValueFactory.fromAnyRef(this.helixTaskId))
        .withValue(GobblinClusterConfigurationKeys.HELIX_PARTITION_ID_KEY, ConfigValueFactory.fromAnyRef(partitionNum));

    Retryer<SingleTask> retryer = RetryerFactory.newInstance(builder.getConfig());

    try {
      eventBus = EventBusFactory.get(ContainerHealthCheckFailureEvent.CONTAINER_HEALTH_CHECK_EVENT_BUS_NAME,
          SharedResourcesBrokerFactory.getImplicitBroker());

      this.task = retryer.call(new Callable<SingleTask>() {
        @Override
        public SingleTask call() {
          return new SingleTask(jobId, workUnitFilePath, jobStateFilePath, builder.getFs(), taskAttemptBuilder,
              stateStores,
              dynamicConfig);
        }
      });
    } catch (Exception e) {
      log.error("Execution in creating a SingleTask-with-retry failed, will create a failing task", e);
      this.task = new SingleFailInCreationTask(jobId, workUnitFilePath, jobStateFilePath, builder.getFs(), taskAttemptBuilder,
          stateStores, dynamicConfig);
    }
  }

  private void getInfoFromTaskConfig() {
    Map<String, String> configMap = this.taskConfig.getConfigMap();
    this.jobName = configMap.get(ConfigurationKeys.JOB_NAME_KEY);
    this.jobId = configMap.get(ConfigurationKeys.JOB_ID_KEY);
    this.helixTaskId = this.taskConfig.getId();
    this.jobKey = Long.toString(Id.parse(this.jobId).getSequence());
    this.taskId = configMap.get(ConfigurationKeys.TASK_ID_KEY);
    this.workUnitFilePath =
        new Path(configMap.get(GobblinClusterConfigurationKeys.WORK_UNIT_FILE_PATH));
  }

  @Override
  public TaskResult run() {
    this.taskMetrics.helixTaskTotalRunning.incrementAndGet();
    long startTime = System.currentTimeMillis();
    log.info("Actual task {} started. [{} {}]", this.taskId, this.applicationName, this.instanceName);
    try (Closer closer = Closer.create()) {
      closer.register(MDC.putCloseable(ConfigurationKeys.JOB_NAME_KEY, this.jobName));
      closer.register(MDC.putCloseable(ConfigurationKeys.JOB_KEY_KEY, this.jobKey));
      this.task.run();
      log.info("Actual task {} completed.", this.taskId);
      this.taskMetrics.helixTaskTotalCompleted.incrementAndGet();
      return new TaskResult(TaskResult.Status.COMPLETED, "");
    } catch (InterruptedException ie) {
      log.error("Interrupting task {}", this.taskId);
      Thread.currentThread().interrupt();
      log.error("Actual task {} interrupted.", this.taskId);
      this.taskMetrics.helixTaskTotalFailed.incrementAndGet();
      return new TaskResult(TaskResult.Status.CANCELED, "");
    } catch (TaskCreationException te) {
      eventBus.post(createTaskCreationEvent("Task Execution"));
      log.error("Actual task {} failed in creation due to {}, will request new container to schedule it",
          this.taskId, te.getMessage());
      this.taskMetrics.helixTaskTotalCancelled.incrementAndGet();
      return new TaskResult(TaskResult.Status.FAILED, "Root cause:" + ExceptionUtils.getRootCauseMessage(te));
    } catch (Throwable t) {
      log.error(String.format("Actual task %s failed due to:", this.taskId), t);
      this.taskMetrics.helixTaskTotalCancelled.incrementAndGet();
      return new TaskResult(TaskResult.Status.FAILED, "");
    } finally {
      this.taskMetrics.helixTaskTotalRunning.decrementAndGet();
      this.taskMetrics.updateTimeForTaskExecution(startTime);
    }
  }

  private ContainerHealthCheckFailureEvent createTaskCreationEvent(String phase) {
    ContainerHealthCheckFailureEvent event = new ContainerHealthCheckFailureEvent(
        ConfigFactory.parseMap(this.taskConfig.getConfigMap()) , getClass().getName());
    event.addMetadata("jobName", this.jobName);
    event.addMetadata("AppName", this.applicationName);
    event.addMetadata(HELIX_INSTANCE_KEY, this.instanceName);
    event.addMetadata(HELIX_JOB_ID_KEY, this.helixJobId);
    event.addMetadata(HELIX_TASK_ID_KEY, this.helixTaskId);
    event.addMetadata("WUPath", this.workUnitFilePath.toString());
    event.addMetadata("Phase", phase);
    return event;
  }

  private Integer getPartitionForHelixTask(TaskDriver taskDriver) {
    //Get Helix partition id for this task
    JobContext jobContext = taskDriver.getJobContext(this.helixJobId);
    if (jobContext != null) {
      return jobContext.getTaskIdPartitionMap().get(this.helixTaskId);
    }
    return null;
  }

  @Override
  public void cancel() {
    log.info("Gobblin helix task cancellation invoked for jobId {}.", jobId);
    if (this.task != null ) {
      try {
        this.task.cancel();
        log.info("Gobblin helix task cancellation completed for jobId {}.", jobId);
      } catch (Throwable t) {
        log.info("Gobblin helix task cancellation for jobId {} failed with exception.", jobId, t);
        Throwables.propagate(t);
      }
    } else {
      log.warn("Cancel called for an uninitialized Gobblin helix task for jobId {}.", jobId);
    }
  }
}
