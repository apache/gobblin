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

package org.apache.gobblin.temporal;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.temporal.ddm.worker.ExecutionWorker;
import org.apache.gobblin.temporal.workflows.helloworld.HelloWorldJobLauncher;
import org.apache.gobblin.temporal.workflows.helloworld.HelloWorldWorker;


/**
 * A central place for configuration related constants of a Gobblin Temporal.
 */
@Alpha
public interface GobblinTemporalConfigurationKeys {

  String PREFIX = "gobblin.temporal.";
  String STAGE_SPECIFIC_PREFIX = PREFIX + "stage.";

  String WORKER_CLASS = PREFIX + "worker.class";
  String DEFAULT_WORKER_CLASS = HelloWorldWorker.class.getName();
  String EXECUTION_WORKER_CLASS = ExecutionWorker.class.getName();
  String GOBBLIN_TEMPORAL_NAMESPACE = PREFIX + "namespace";
  String DEFAULT_GOBBLIN_TEMPORAL_NAMESPACE = PREFIX + "namespace";

  String GOBBLIN_TEMPORAL_TASK_QUEUE = PREFIX + "task.queue.name";
  String DEFAULT_GOBBLIN_TEMPORAL_TASK_QUEUE = "GobblinTemporalTaskQueue";

  // Execution task queue for work execution specialization
  String EXECUTION_TASK_QUEUE = PREFIX + "execution.task.queue.name";
  String DEFAULT_EXECUTION_TASK_QUEUE = "GobblinTemporalExecutionQueue";
  String GOBBLIN_TEMPORAL_JOB_LAUNCHER_PREFIX = PREFIX + "job.launcher.";
  String GOBBLIN_TEMPORAL_JOB_LAUNCHER_CLASS = GOBBLIN_TEMPORAL_JOB_LAUNCHER_PREFIX + "class";
  String DEFAULT_GOBBLIN_TEMPORAL_JOB_LAUNCHER_CLASS = HelloWorldJobLauncher.class.getName();

  String GOBBLIN_TEMPORAL_JOB_LAUNCHER_ARG_PREFIX = GOBBLIN_TEMPORAL_JOB_LAUNCHER_PREFIX + "arg.";
  String GOBBLIN_TEMPORAL_JOB_LAUNCHER_CONFIG_OVERRIDES = GOBBLIN_TEMPORAL_JOB_LAUNCHER_PREFIX + "config.overrides";
  String GOBBLIN_TEMPORAL_WORK_DIR_CLEANUP_ENABLED = PREFIX + "work.dir.cleanup.enabled";
  String DEFAULT_GOBBLIN_TEMPORAL_WORK_DIR_CLEANUP_ENABLED = "true";
  String WORK_DIR_PATHS_TO_DELETE = PREFIX + "work.dir.paths.to.delete";
  String SHUTDOWN_CLEANUP_TIMEOUT_SECONDS = PREFIX + "shutdown.cleanup.timeout.seconds";
  String DEFAULT_SHUTDOWN_CLEANUP_TIMEOUT_SECONDS = "600";

  String GOBBLIN_TEMPORAL_CONTAINER_METRICS_PREFIX = PREFIX + "container.metrics.";
  String GOBBLIN_TEMPORAL_CONTAINER_METRICS_APPLICATION_NAME = GOBBLIN_TEMPORAL_CONTAINER_METRICS_PREFIX + "application.name";
  String GOBBLIN_TEMPORAL_CONTAINER_METRICS_TASK_RUNNER_ID = GOBBLIN_TEMPORAL_CONTAINER_METRICS_PREFIX + "task.runner.id";

  /**
   * Suffix for metrics emitted by GobblinTemporalJobLauncher for preventing collisions with prod jobs
   * during testing
   *
   */
  String GOBBLIN_TEMPORAL_JOB_METRICS_SUFFIX = PREFIX + "job.metrics.suffix";
  /**
   * Default suffix for metrics emitted by GobblinTemporalJobLauncher for preventing collisions with prod jobs
   * is not empty because temporal is still in alpha stages, and should not accidentally affect a prod job
   */
  String DEFAULT_GOBBLIN_TEMPORAL_JOB_METRICS_SUFFIX = "-temporal";

  /**
   * Number of worker processes to spin up per task runner
   * NOTE: If this size is too large, your container can OOM and halt execution unexpectedly. It's recommended not to touch
   * this parameter
   */
  String TEMPORAL_NUM_WORKERS_PER_CONTAINER = PREFIX + "num.workers.per.container";
  int DEFAULT_TEMPORAL_NUM_WORKERS_PER_CONTAINERS = 1;
  String TEMPORAL_NUM_THREADS_PER_WORKER = PREFIX + "num.threads.per.worker";
  int DEFAULT_TEMPORAL_NUM_THREADS_PER_WORKER = 15;
  String TEMPORAL_NUM_THREADS_PER_EXECUTION_WORKER = PREFIX + "num.threads.per.execution.worker";

  // Concurrency configs for WorkFulfillmentWorker
  String TEMPORAL_MAX_CONCURRENT_ACTIVITY_SIZE = PREFIX + "max.concurrent.activity.size";
  String TEMPORAL_MAX_CONCURRENT_LOCAL_ACTIVITY_SIZE = PREFIX + "max.concurrent.local.activity.size";
  String TEMPORAL_MAX_CONCURRENT_WORKFLOW_TASK_SIZE = PREFIX + "max.concurrent.workflow.task.size";

  // Concurrency configs for ExecutionWorker
  String TEMPORAL_EXECUTION_MAX_CONCURRENT_ACTIVITY_SIZE = PREFIX + "execution.max.concurrent.activity.size";
  String TEMPORAL_EXECUTION_MAX_CONCURRENT_LOCAL_ACTIVITY_SIZE = PREFIX + "execution.max.concurrent.local.activity.size";
  String TEMPORAL_EXECUTION_MAX_CONCURRENT_WORKFLOW_TASK_SIZE = PREFIX + "execution.max.concurrent.workflow.task.size";

  // Configuration key for setting the amortized throughput per worker thread per minute
  String TEMPORAL_WORKER_THREAD_AMORTIZED_THROUGHPUT_PER_MINUTE = PREFIX + "worker.thread.amortized.throughput.per.minute";
  long DEFAULT_TEMPORAL_WORKER_THREAD_AMORTIZED_THROUGHPUT_PER_MINUTE = 500 * 1000 * 1000L; // 500MB/min

  // Configuration key for setting the maximum number of work units allowed per container
  String TEMPORAL_WORKUNITS_PER_CONTAINER = PREFIX + "workunits.per.container";
  int DEFAULT_MAX_WORKUNITS_PER_CONTAINER = 2000;

  String TEMPORAL_CONNECTION_STRING = PREFIX + "connection.string";

  /**
   * Prefix for Gobblin-on-Temporal Dynamic Scaling
   */
  String DYNAMIC_SCALING_PREFIX = PREFIX + "dynamic.scaling.";

  // Configuration key to enable/disable dynamic scaling
  String DYNAMIC_SCALING_ENABLED = DYNAMIC_SCALING_PREFIX + "enabled";

  String DYNAMIC_SCALING_POLLING_INTERVAL_SECS = DYNAMIC_SCALING_PREFIX + "polling.interval.seconds";
  int DEFAULT_DYNAMIC_SCALING_POLLING_INTERVAL_SECS = 60;

  /**
   * Temporal metrics config properties
   */
  String TEMPORAL_METRICS_PREFIX = PREFIX + "metrics.";
  String TEMPORAL_METRICS_OTLP_PREFIX_WITHOUT_DOT = TEMPORAL_METRICS_PREFIX + "otlp";
  String TEMPORAL_METRICS_OTLP_HEADERS_KEY = TEMPORAL_METRICS_OTLP_PREFIX_WITHOUT_DOT + ".headers";
  String TEMPORAL_METRICS_REPORT_INTERVAL_SECS = TEMPORAL_METRICS_OTLP_PREFIX_WITHOUT_DOT + ".report.interval.seconds";
  int DEFAULT_TEMPORAL_METRICS_REPORT_INTERVAL_SECS = 10;
  String TEMPORAL_METRICS_OTLP_DIMENSIONS_KEY = TEMPORAL_METRICS_OTLP_PREFIX_WITHOUT_DOT + ".dimensions";

  /**
   * Activities timeout configs
   */
  String TEMPORAL_ACTIVITY_HEARTBEAT_TIMEOUT_MINUTES = PREFIX + "activity.heartbeat.timeout.minutes";
  int DEFAULT_TEMPORAL_ACTIVITY_HEARTBEAT_TIMEOUT_MINUTES = 5;
  String TEMPORAL_ACTIVITY_HEARTBEAT_INTERVAL_MINUTES = PREFIX + "activity.heartbeat.interval.minutes";
  int DEFAULT_TEMPORAL_ACTIVITY_HEARTBEAT_INTERVAL_MINUTES = 1;
  String ACTIVITY_STARTTOCLOSE_TIMEOUT_MINUTES = "activity.starttoclose.timeout.minutes";
  int DEFAULT_TEMPORAL_ACTIVITY_STARTTOCLOSE_TIMEOUT_MINUTES = 360;
  String TEMPORAL_GENERATE_WORKUNITS_ACTIVITY_STARTTOCLOSE_TIMEOUT_MINUTES =
      PREFIX + "generate.workunits." + ACTIVITY_STARTTOCLOSE_TIMEOUT_MINUTES;
  String TEMPORAL_RECOMMEND_SCALING_ACTIVITY_STARTTOCLOSE_TIMEOUT_MINUTES =
      PREFIX + "recommend.scaling." + ACTIVITY_STARTTOCLOSE_TIMEOUT_MINUTES;
  String TEMPORAL_DELETE_WORK_DIRS_ACTIVITY_STARTTOCLOSE_TIMEOUT_MINUTES =
      PREFIX + "delete.work.dirs." + ACTIVITY_STARTTOCLOSE_TIMEOUT_MINUTES;
  String TEMPORAL_PROCESS_WORKUNIT_ACTIVITY_STARTTOCLOSE_TIMEOUT_MINUTES =
      PREFIX + "process.workunit." + ACTIVITY_STARTTOCLOSE_TIMEOUT_MINUTES;
  String TEMPORAL_COMMIT_ACTIVITY_STARTTOCLOSE_TIMEOUT_MINUTES =
      PREFIX + "commit." + ACTIVITY_STARTTOCLOSE_TIMEOUT_MINUTES;
  String TEMPORAL_SUBMIT_GTE_ACTIVITY_STARTTOCLOSE_TIMEOUT_MINUTES =
      PREFIX + "submit.gte." + ACTIVITY_STARTTOCLOSE_TIMEOUT_MINUTES;
  String TEMPORAL_EMIT_OTEL_METRICS_ACTIVITY_STARTTOCLOSE_TIMEOUT_MINUTES =
      PREFIX + "emit.otel.metrics." + ACTIVITY_STARTTOCLOSE_TIMEOUT_MINUTES;
  String TEMPORAL_ACTIVITY_RETRY_OPTIONS = PREFIX + "activity.retry.options.";
  String TEMPORAL_ACTIVITY_RETRY_OPTIONS_INITIAL_INTERVAL_SECONDS = TEMPORAL_ACTIVITY_RETRY_OPTIONS + "initial.interval.seconds";
  int DEFAULT_TEMPORAL_ACTIVITY_RETRY_OPTIONS_INITIAL_INTERVAL_SECONDS = 3;
  String TEMPORAL_ACTIVITY_RETRY_OPTIONS_MAXIMUM_INTERVAL_SECONDS = TEMPORAL_ACTIVITY_RETRY_OPTIONS + "maximum.interval.seconds";
  int DEFAULT_TEMPORAL_ACTIVITY_RETRY_OPTIONS_MAXIMUM_INTERVAL_SECONDS = 100;
  String TEMPORAL_ACTIVITY_RETRY_OPTIONS_BACKOFF_COEFFICIENT = TEMPORAL_ACTIVITY_RETRY_OPTIONS + "backoff.coefficient";
  double DEFAULT_TEMPORAL_ACTIVITY_RETRY_OPTIONS_BACKOFF_COEFFICIENT = 2;
  String TEMPORAL_ACTIVITY_RETRY_OPTIONS_MAXIMUM_ATTEMPTS = TEMPORAL_ACTIVITY_RETRY_OPTIONS + "maximum.attempts";
  int DEFAULT_TEMPORAL_ACTIVITY_RETRY_OPTIONS_MAXIMUM_ATTEMPTS = 4;

  /**
   * Memory allocation for execution worker containers.
   */
  String WORK_EXECUTION_MEMORY_MB = STAGE_SPECIFIC_PREFIX + "workExecution.memory.mb";
}
