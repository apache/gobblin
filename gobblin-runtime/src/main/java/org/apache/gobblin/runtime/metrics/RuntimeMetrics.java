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

package org.apache.gobblin.runtime.metrics;

import org.apache.gobblin.metrics.ServiceMetricNames;


/**
 * Constants used for naming {@link org.apache.gobblin.metrics.Metric}s and metric metadata in gobblin-runtime.
 */
public class RuntimeMetrics {

  // Metric names
  public static final String GOBBLIN_KAFKA_HIGH_LEVEL_CONSUMER_MESSAGES_READ =
      "gobblin.kafka.highLevelConsumer.messagesRead";
  public static final String GOBBLIN_KAFKA_HIGH_LEVEL_CONSUMER_QUEUE_SIZE_PREFIX = "gobblin.kafka.highLevelConsumer.queueSize";
  public static final String GOBBLIN_JOB_MONITOR_KAFKA_TOTAL_SPECS = "gobblin.jobMonitor.kafka.totalSpecs";
  public static final String GOBBLIN_JOB_MONITOR_KAFKA_NEW_SPECS = "gobblin.jobMonitor.kafka.newSpecs";
  public static final String GOBBLIN_JOB_MONITOR_KAFKA_UPDATED_SPECS = "gobblin.jobMonitor.kafka.updatedSpecs";
  public static final String GOBBLIN_JOB_MONITOR_KAFKA_REMOVED_SPECS = "gobblin.jobMonitor.kafka.removedSpecs";
  public static final String GOBBLIN_JOB_MONITOR_KAFKA_CANCELLED_SPECS = "gobblin.jobMonitor.kafka.cancelledSpecs";
  public static final String GOBBLIN_JOB_MONITOR_SLAEVENT_REJECTEDEVENTS = "gobblin.jobMonitor.slaevent.rejectedevents";
  public static final String GOBBLIN_JOB_MONITOR_KAFKA_MESSAGE_PARSE_FAILURES =
      "gobblin.jobMonitor.kafka.messageParseFailures";
  public static final String GOBBLIN_SPEC_STORE_MONITOR_SUCCESSFULLY_ADDED_SPECS = ServiceMetricNames.GOBBLIN_SERVICE_PREFIX + ".specStoreMonitor.successful.added.specs";
  public static final String GOBBLIN_SPEC_STORE_MONITOR_FAILED_ADDED_SPECS = ServiceMetricNames.GOBBLIN_SERVICE_PREFIX + ".specStoreMonitor.failed.added.specs";
  public static final String GOBBLIN_SPEC_STORE_MONITOR_DELETED_SPECS = ServiceMetricNames.GOBBLIN_SERVICE_PREFIX + ".specStoreMonitor.deleted.specs";
  public static final String GOBBLIN_SPEC_STORE_MONITOR_UNEXPECTED_ERRORS = ServiceMetricNames.GOBBLIN_SERVICE_PREFIX + ".specStoreMonitor.unexpected.errors";
  public static final String GOBBLIN_SPEC_STORE_MESSAGE_PROCESSED= ServiceMetricNames.GOBBLIN_SERVICE_PREFIX + ".specStoreMonitor.message.processed";
  public static final String GOBBLIN_SPEC_STORE_PRODUCE_TO_CONSUME_DELAY_MILLIS =
      ServiceMetricNames.GOBBLIN_SERVICE_PREFIX + ".specstoreMonitor.produce.to.consume.delay";
  public static final String DAG_ACTION_STORE_MONITOR_PREFIX = ServiceMetricNames.GOBBLIN_SERVICE_PREFIX + ".dagActionStoreMonitor";
  public static final String GOBBLIN_DAG_ACTION_STORE_MONITOR_KILLS_INVOKED = DAG_ACTION_STORE_MONITOR_PREFIX + ".kills.invoked";
  public static final String GOBBLIN_DAG_ACTION_STORE_MONITOR_MESSAGE_PROCESSED = DAG_ACTION_STORE_MONITOR_PREFIX + ".message.processed";
  public static final String GOBBLIN_DAG_ACTION_STORE_MONITOR_MESSAGES_FILTERED_OUT = DAG_ACTION_STORE_MONITOR_PREFIX + ".messagesFilteredOut";
  public static final String GOBBLIN_DAG_ACTION_STORE_MONITOR_RESUMES_INVOKED = DAG_ACTION_STORE_MONITOR_PREFIX + ".resumes.invoked";
  public static final String GOBBLIN_DAG_ACTION_STORE_MONITOR_FLOWS_LAUNCHED = DAG_ACTION_STORE_MONITOR_PREFIX + ".flows.launched";

  public static final String GOBBLIN_DAG_ACTION_STORE_FAILED_FLOW_LAUNCHED_SUBMISSIONS = DAG_ACTION_STORE_MONITOR_PREFIX + ".failedFlowLaunchSubmissions";
  public static final String GOBBLIN_DAG_ACTION_STORE_MONITOR_UNEXPECTED_ERRORS = DAG_ACTION_STORE_MONITOR_PREFIX + ".unexpected.errors";
  public static final String
      GOBBLIN_DAG_ACTION_STORE_PRODUCE_TO_CONSUME_DELAY_MILLIS = DAG_ACTION_STORE_MONITOR_PREFIX + ".produce.to.consume.delay";
  public static final String GOBBLIN_MYSQL_QUOTA_MANAGER_UNEXPECTED_ERRORS = ServiceMetricNames.GOBBLIN_SERVICE_PREFIX + "gobblin.mysql.quota.manager.unexpected.errors";
  public static final String GOBBLIN_MYSQL_QUOTA_MANAGER_QUOTA_REQUESTS_EXCEEDED = ServiceMetricNames.GOBBLIN_SERVICE_PREFIX + "gobblin.mysql.quota.manager.quotaRequests.exceeded";
  public static final String GOBBLIN_MYSQL_QUOTA_MANAGER_TIME_TO_CHECK_QUOTA = ServiceMetricNames.GOBBLIN_SERVICE_PREFIX + "gobblin.mysql.quota.manager.time.to.check.quota";

  // The following metrics are used to identify the bottlenecks for initializing the job scheduler
  public static final String
      GOBBLIN_JOB_SCHEDULER_GET_SPECS_DURING_STARTUP_PER_SPEC_RATE_NANOS = ServiceMetricNames.GOBBLIN_SERVICE_PREFIX + ".jobScheduler.getSpecsDuringStartupPerSpecRateNanos";
  public static final String GOBBLIN_JOB_SCHEDULER_LOAD_SPECS_BATCH_SIZE = ServiceMetricNames.GOBBLIN_SERVICE_PREFIX + ".jobScheduler.loadSpecBatchSize";
  public static final String
      GOBBLIN_JOB_SCHEDULER_TIME_TO_INITIALIZE_SCHEDULER_NANOS = ServiceMetricNames.GOBBLIN_SERVICE_PREFIX + ".jobScheduler.timeToInitializeSchedulerNanos";
  public static final String
      GOBBLIN_JOB_SCHEDULER_TIME_TO_OBTAIN_SPEC_URIS_NANOS = ServiceMetricNames.GOBBLIN_SERVICE_PREFIX + ".jobScheduler.timeToObtainSpecUrisNanos";
  public static final String
      GOBBLIN_JOB_SCHEDULER_INDIVIDUAL_GET_SPEC_SPEED_NANOS = ServiceMetricNames.GOBBLIN_SERVICE_PREFIX + ".jobScheduler.individualGetSpecSpeedNanos";
  public static final String
      GOBBLIN_JOB_SCHEDULER_EACH_COMPLETE_ADD_SPEC_NANOS = ServiceMetricNames.GOBBLIN_SERVICE_PREFIX + ".jobScheduler.eachCompleteAddSpecNanos";
  public static final String
      GOBBLIN_JOB_SCHEDULER_EACH_SPEC_COMPILATION_NANOS = ServiceMetricNames.GOBBLIN_SERVICE_PREFIX + ".jobScheduler.eachSpecCompilationNanos";
  public static final String GOBBLIN_JOB_SCHEDULER_EACH_SCHEDULE_JOB_NANOS = ServiceMetricNames.GOBBLIN_SERVICE_PREFIX + ".jobScheduler.eachScheduleJobNanos";
  public static final String GOBBLIN_JOB_SCHEDULER_TOTAL_GET_SPEC_TIME_NANOS = ServiceMetricNames.GOBBLIN_SERVICE_PREFIX + ".jobScheduler.totalGetSpecTimeNanos";
  public static final String GOBBLIN_JOB_SCHEDULER_TOTAL_ADD_SPEC_TIME_NANOS = ServiceMetricNames.GOBBLIN_SERVICE_PREFIX + ".jobScheduler.totalAddSpecTimeNanos";
  public static final String GOBBLIN_JOB_SCHEDULER_NUM_JOBS_SCHEDULED_DURING_STARTUP = ServiceMetricNames.GOBBLIN_SERVICE_PREFIX + ".jobScheduler.numJobsScheduledDuringStartup";

  // Metadata keys
  public static final String TOPIC = "topic";
  public static final String GROUP_ID = "groupId";
  public static final String SCHEMA = "schema";
}
