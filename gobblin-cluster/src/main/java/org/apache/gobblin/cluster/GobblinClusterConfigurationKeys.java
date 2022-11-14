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

import org.apache.gobblin.annotation.Alpha;


/**
 * A central place for configuration related constants of a Gobblin Cluster.
 *
 * @author Yinan Li
 */
@Alpha
public class GobblinClusterConfigurationKeys {

  public static final String GOBBLIN_CLUSTER_PREFIX = "gobblin.cluster.";

  // Task separation properties
  public static final String ENABLE_TASK_IN_SEPARATE_PROCESS =
      GOBBLIN_CLUSTER_PREFIX + "enableTaskInSeparateProcess";
  public static final String TASK_CLASSPATH =
      GOBBLIN_CLUSTER_PREFIX + "task.classpath";
  public static final String TASK_LOG_CONFIG =
      GOBBLIN_CLUSTER_PREFIX + "task.log.config";
  public static final String TASK_JVM_OPTIONS =
      GOBBLIN_CLUSTER_PREFIX + "task.jvm.options";

  // General Gobblin Cluster application configuration properties.
  public static final String APPLICATION_NAME_OPTION_NAME = "app_name";
  public static final String APPLICATION_ID_OPTION_NAME = "app_id";
  public static final String STANDALONE_CLUSTER_MODE = "standalone_cluster";
  public static final String STANDALONE_CLUSTER_MODE_KEY = GOBBLIN_CLUSTER_PREFIX + "standaloneMode";
  public static final boolean DEFAULT_STANDALONE_CLUSTER_MODE = false;
  // Root working directory for Gobblin cluster
  public static final String CLUSTER_WORK_DIR = GOBBLIN_CLUSTER_PREFIX + "workDir";

  public static final String DISTRIBUTED_JOB_LAUNCHER_ENABLED = GOBBLIN_CLUSTER_PREFIX + "distributedJobLauncherEnabled";
  public static final boolean DEFAULT_DISTRIBUTED_JOB_LAUNCHER_ENABLED = false;
  public static final String DISTRIBUTED_JOB_LAUNCHER_BUILDER = GOBBLIN_CLUSTER_PREFIX + "distributedJobLauncherBuilder";

  // Helix configuration properties.
  public static final String DEDICATED_JOB_CLUSTER_CONTROLLER_ENABLED = GOBBLIN_CLUSTER_PREFIX + "dedicatedJobClusterController.enabled";
  public static final String HELIX_CLUSTER_NAME_KEY = GOBBLIN_CLUSTER_PREFIX + "helix.cluster.name";

  public static final String MANAGER_CLUSTER_NAME_KEY = GOBBLIN_CLUSTER_PREFIX + "manager.cluster.name";
  public static final String DEDICATED_MANAGER_CLUSTER_ENABLED = GOBBLIN_CLUSTER_PREFIX + "dedicatedManagerCluster.enabled";

  public static final String DEDICATED_TASK_DRIVER_CLUSTER_CONTROLLER_ENABLED = GOBBLIN_CLUSTER_PREFIX + "dedicatedTaskDriverClusterController.enabled";
  public static final String TASK_DRIVER_CLUSTER_NAME_KEY = GOBBLIN_CLUSTER_PREFIX + "taskDriver.cluster.name";
  public static final String DEDICATED_TASK_DRIVER_CLUSTER_ENABLED = GOBBLIN_CLUSTER_PREFIX + "dedicatedTaskDriverCluster.enabled";
  public static final String TASK_DRIVER_ENABLED = GOBBLIN_CLUSTER_PREFIX + "taskDriver.enabled";

  public static final String ZK_CONNECTION_STRING_KEY = GOBBLIN_CLUSTER_PREFIX + "zk.connection.string";
  public static final String WORK_UNIT_FILE_PATH = GOBBLIN_CLUSTER_PREFIX + "work.unit.file.path";
  public static final String HELIX_INSTANCE_NAME_OPTION_NAME = "helix_instance_name";
  public static final String HELIX_INSTANCE_NAME_KEY = GOBBLIN_CLUSTER_PREFIX + "helixInstanceName";

  public static final String HELIX_INSTANCE_TAGS_OPTION_NAME = "helix_instance_tags";

  // The number of tasks that can be running concurrently in the same worker process
  public static final String HELIX_CLUSTER_TASK_CONCURRENCY = GOBBLIN_CLUSTER_PREFIX + "helix.taskConcurrency";
  public static final int HELIX_CLUSTER_TASK_CONCURRENCY_DEFAULT = 40;

  // Should job be executed in the scheduler thread?
  public static final String JOB_EXECUTE_IN_SCHEDULING_THREAD = GOBBLIN_CLUSTER_PREFIX + "job.executeInSchedulingThread";
  public static final boolean JOB_EXECUTE_IN_SCHEDULING_THREAD_DEFAULT = true;

  // Helix tagging
  public static final String HELIX_JOB_TAG_KEY = GOBBLIN_CLUSTER_PREFIX + "helixJobTag";
  public static final String HELIX_PLANNING_JOB_TAG_KEY = GOBBLIN_CLUSTER_PREFIX + "helixPlanningJobTag";
  public static final String HELIX_INSTANCE_TAGS_KEY = GOBBLIN_CLUSTER_PREFIX + "helixInstanceTags";
  public static final String HELIX_DEFAULT_TAG = "GobblinHelixDefaultTag";

  // Helix job quota
  public static final String HELIX_JOB_TYPE_KEY = GOBBLIN_CLUSTER_PREFIX + "helixJobType";
  public static final String HELIX_PLANNING_JOB_TYPE_KEY = GOBBLIN_CLUSTER_PREFIX + "helixPlanningJobType";

  // Planning job properties
  public static final String PLANNING_JOB_NAME_PREFIX = "PlanningJob";
  public static final String PLANNING_CONF_PREFIX = GOBBLIN_CLUSTER_PREFIX + "planning.";
  public static final String PLANNING_ID_KEY = PLANNING_CONF_PREFIX + "idKey";
  public static final String PLANNING_JOB_CREATE_TIME = PLANNING_CONF_PREFIX + "createTime";

  // Actual job properties
  public static final String ACTUAL_JOB_NAME_PREFIX = "ActualJob";

  // job spec operation
  public static final String JOB_ALWAYS_DELETE = GOBBLIN_CLUSTER_PREFIX + "job.alwaysDelete";

  // Job quota configuration as a comma separated list of name value pairs separated by a colon.
  // Example: A:1,B:38,DEFAULT:1
  public static final String HELIX_TASK_QUOTA_CONFIG_KEY = "gobblin.cluster.helixTaskQuotaConfig";

  /**
   * A path pointing to a directory that contains job execution files to be executed by Gobblin. This directory can
   * have a nested structure.
   *
   * @see <a href="https://gobblin.readthedocs.io/en/latest/user-guide/Working-with-Job-Configuration-Files/">Job Config Files</a>
   */
  public static final String JOB_CONF_PATH_KEY = GOBBLIN_CLUSTER_PREFIX + "job.conf.path";
  //A java.util.regex specifying the subset of jobs under JOB_CONF_PATH to be run.
  public static final String JOBS_TO_RUN = GOBBLIN_CLUSTER_PREFIX + "jobsToRun";
  public static final String INPUT_WORK_UNIT_DIR_NAME = "_workunits";
  public static final String OUTPUT_TASK_STATE_DIR_NAME = "_taskstates";
  // This is the directory to store job.state files when a state store is used.
  // Note that a .job.state file is not the same thing as a .jst file.
  public static final String JOB_STATE_DIR_NAME = "_jobstates";
  public static final String TAR_GZ_FILE_SUFFIX = ".tar.gz";

  // Other misc configuration properties.
  public static final String TASK_SUCCESS_OPTIONAL_KEY = "TASK_SUCCESS_OPTIONAL";
  public static final String GOBBLIN_CLUSTER_LOG4J_CONFIGURATION_FILE = "log4j-cluster.properties";
  public static final String JOB_CONFIGURATION_MANAGER_KEY = GOBBLIN_CLUSTER_PREFIX + "job.configuration.manager";

  public static final String JOB_SPEC_REFRESH_INTERVAL = GOBBLIN_CLUSTER_PREFIX + "job.spec.refresh.interval";
  public static final String JOB_SPEC_URI = GOBBLIN_CLUSTER_PREFIX + "job.spec.uri";

  public static final String SPEC_CONSUMER_CLASS_KEY = GOBBLIN_CLUSTER_PREFIX + "specConsumer.class";
  public static final String DEFAULT_SPEC_CONSUMER_CLASS =
      "org.apache.gobblin.service.SimpleKafkaSpecConsumer";
  public static final String DEFAULT_STREAMING_SPEC_CONSUMER_CLASS =
      "org.apache.gobblin.service.StreamingKafkaSpecConsumer";
  public static final String JOB_CATALOG_KEY = GOBBLIN_CLUSTER_PREFIX + "job.catalog";
  public static final String DEFAULT_JOB_CATALOG =
      "org.apache.gobblin.runtime.job_catalog.NonObservingFSJobCatalog";

  public static final String STOP_TIMEOUT_SECONDS = GOBBLIN_CLUSTER_PREFIX + "stopTimeoutSeconds";
  public static final long DEFAULT_STOP_TIMEOUT_SECONDS = 60;

  public static final String HELIX_WORKFLOW_EXPIRY_TIME_SECONDS = GOBBLIN_CLUSTER_PREFIX + "workflow.expirySeconds";
  public static final long DEFAULT_HELIX_WORKFLOW_EXPIRY_TIME_SECONDS = 6 * 60 * 60;

  public static final String HELIX_JOB_STOP_TIMEOUT_SECONDS = GOBBLIN_CLUSTER_PREFIX + "helix.job.stopTimeoutSeconds";
  public static final long DEFAULT_HELIX_JOB_STOP_TIMEOUT_SECONDS = 10L;
  public static final String TASK_RUNNER_SUITE_BUILDER = GOBBLIN_CLUSTER_PREFIX + "taskRunnerSuite.builder";

  public static final String HELIX_JOB_NAME_KEY = GOBBLIN_CLUSTER_PREFIX + "helixJobName";
  public static final String HELIX_JOB_TIMEOUT_ENABLED_KEY = "helix.job.timeout.enabled";
  public static final String DEFAULT_HELIX_JOB_TIMEOUT_ENABLED = "false";
  public static final String HELIX_JOB_TIMEOUT_SECONDS = "helix.job.timeout.seconds";
  public static final String DEFAULT_HELIX_JOB_TIMEOUT_SECONDS = "10800";
  public static final String HELIX_TASK_NAME_KEY = GOBBLIN_CLUSTER_PREFIX + "helixTaskName";
  public static final String HELIX_TASK_TIMEOUT_SECONDS = "helix.task.timeout.seconds";
  public static final String HELIX_TASK_MAX_ATTEMPTS_KEY = "helix.task.maxAttempts";

  public static final String HELIX_WORKFLOW_DELETE_TIMEOUT_SECONDS = GOBBLIN_CLUSTER_PREFIX + "workflowDeleteTimeoutSeconds";
  public static final long DEFAULT_HELIX_WORKFLOW_DELETE_TIMEOUT_SECONDS = 300;

  public static final String HELIX_WORKFLOW_LISTING_TIMEOUT_SECONDS = GOBBLIN_CLUSTER_PREFIX + "workflowListingTimeoutSeconds";
  public static final long DEFAULT_HELIX_WORKFLOW_LISTING_TIMEOUT_SECONDS = 60;

  public static final String CLEAN_ALL_DIST_JOBS = GOBBLIN_CLUSTER_PREFIX + "bootup.clean.dist.jobs";
  public static final boolean DEFAULT_CLEAN_ALL_DIST_JOBS = false;

  public static final String NON_BLOCKING_PLANNING_JOB_ENABLED = GOBBLIN_CLUSTER_PREFIX + "nonBlocking.planningJob.enabled";
  public static final boolean DEFAULT_NON_BLOCKING_PLANNING_JOB_ENABLED = false;
  public static final String KILL_DUPLICATE_PLANNING_JOB = GOBBLIN_CLUSTER_PREFIX + "kill.duplicate.planningJob";
  public static final boolean DEFAULT_KILL_DUPLICATE_PLANNING_JOB = true;

  public static final String CANCEL_RUNNING_JOB_ON_DELETE = GOBBLIN_CLUSTER_PREFIX + "job.cancelRunningJobOnDelete";
  public static final String DEFAULT_CANCEL_RUNNING_JOB_ON_DELETE = "false";

  // By default we cancel job by calling helix stop API. In some cases, jobs just hang in STOPPING state and preventing
  // new job being launched. We provide this config to give an option to cancel jobs by calling Delete API. Directly delete
  // a Helix workflow should be safe in Gobblin world, as Gobblin job is stateless for Helix since we implement our own state store
  public static final String CANCEL_HELIX_JOB_BY_DELETE = GOBBLIN_CLUSTER_PREFIX + "job.cancelHelixJobByDelete";
  public static final boolean DEFAULT_CANCEL_HELIX_JOB_BY_DELETE = false;

  public static final String HELIX_JOB_STOPPING_STATE_TIMEOUT_SECONDS = GOBBLIN_CLUSTER_PREFIX + "job.stoppingStateTimeoutSeconds";
  public static final long DEFAULT_HELIX_JOB_STOPPING_STATE_TIMEOUT_SECONDS = 300;
  public static final String CONTAINER_HEALTH_METRICS_SERVICE_ENABLED = GOBBLIN_CLUSTER_PREFIX + "container.health.metrics.service.enabled" ;
  public static final boolean DEFAULT_CONTAINER_HEALTH_METRICS_SERVICE_ENABLED = false;

  //Config to enable/disable container "suicide" on health check failures. To be used in execution modes, where the exiting
  // container can be replaced with another container e.g. Gobblin-on-Yarn mode.
  public static final String CONTAINER_EXIT_ON_HEALTH_CHECK_FAILURE_ENABLED = GOBBLIN_CLUSTER_PREFIX + "container.exitOnHealthCheckFailure";
  public static final boolean DEFAULT_CONTAINER_EXIT_ON_HEALTH_CHECK_FAILURE_ENABLED = false;

  // Config to specify the resource requirement for each Gobblin job run, so that helix tasks within this job will
  // be assigned to containers with desired resource. This config need to cooperate with helix job tag, so that helix
  // cluster knows how to distribute tasks to correct containers.
  public static final String HELIX_JOB_CONTAINER_MEMORY_MBS = GOBBLIN_CLUSTER_PREFIX + "job.container.memory.mbs";
  public static final String HELIX_JOB_CONTAINER_CORES = GOBBLIN_CLUSTER_PREFIX + "job.container.cores";



  //Config to enable/disable reuse of existing Helix Cluster
  public static final String HELIX_CLUSTER_OVERWRITE_KEY = GOBBLIN_CLUSTER_PREFIX + "helix.overwrite";
  public static final boolean DEFAULT_HELIX_CLUSTER_OVERWRITE = true;

  //Config to enable/disable cluster creation. Should set this config to false if Helix-as-a-Service is used to manage
  // the cluster
  public static final String IS_HELIX_CLUSTER_MANAGED = GOBBLIN_CLUSTER_PREFIX + "isHelixClusterManaged";
  public static final boolean DEFAULT_IS_HELIX_CLUSTER_MANAGED = false;

  public static final String HADOOP_CONFIG_OVERRIDES_PREFIX = GOBBLIN_CLUSTER_PREFIX + "hadoop.inject";

  //Configurations that will be set dynamically when a GobblinTaskRunner/GobblinHelixTask are instantiated.
  public static final String GOBBLIN_HELIX_PREFIX = "gobblin.helix.";
  public static final String HELIX_JOB_ID_KEY = GOBBLIN_HELIX_PREFIX + "jobId";
  public static final String HELIX_TASK_ID_KEY = GOBBLIN_HELIX_PREFIX + "taskId";
  public static final String HELIX_PARTITION_ID_KEY = GOBBLIN_HELIX_PREFIX + "partitionId" ;
  public static final String TASK_RUNNER_HOST_NAME_KEY = GOBBLIN_HELIX_PREFIX + "hostName";
  public static final String CONTAINER_ID_KEY = GOBBLIN_HELIX_PREFIX + "containerId";

  public static final String GOBBLIN_CLUSTER_SYSTEM_PROPERTY_PREFIX = GOBBLIN_CLUSTER_PREFIX + "sysProps";
}