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
  public static final String GOBBLIN_JOB_MONITOR_KAFKA_TOTAL_SPECS = "gobblin.jobMonitor.kafka.totalSpecs";
  public static final String GOBBLIN_JOB_MONITOR_KAFKA_NEW_SPECS = "gobblin.jobMonitor.kafka.newSpecs";
  public static final String GOBBLIN_JOB_MONITOR_KAFKA_UPDATED_SPECS = "gobblin.jobMonitor.kafka.updatedSpecs";
  public static final String GOBBLIN_JOB_MONITOR_KAFKA_REMOVED_SPECS = "gobblin.jobMonitor.kafka.removedSpecs";
  public static final String GOBBLIN_JOB_MONITOR_KAFKA_CANCELLED_SPECS = "gobblin.jobMonitor.kafka.cancelledSpecs";
  public static final String GOBBLIN_JOB_MONITOR_SLAEVENT_REJECTEDEVENTS = "gobblin.jobMonitor.slaevent.rejectedevents";
  public static final String GOBBLIN_JOB_MONITOR_KAFKA_MESSAGE_PARSE_FAILURES =
      "gobblin.jobMonitor.kafka.messageParseFailures";
  public static final String GOBBLIN_SPEC_STORE_MONITOR_SUCCESSFULLY_ADDED_SPECS = ServiceMetricNames.GOBBLIN_SERVICE_PREFIX + "gobblin.specStoreMonitor.successful.added.specs";
  public static final String GOBBLIN_SPEC_STORE_MONITOR_FAILED_ADDED_SPECS = ServiceMetricNames.GOBBLIN_SERVICE_PREFIX + "gobblin.specStoreMonitor.failed.added.specs";
  public static final String GOBBLIN_SPEC_STORE_MONITOR_DELETED_SPECS = ServiceMetricNames.GOBBLIN_SERVICE_PREFIX + "gobblin.specStoreMonitor.deleted.specs";
  public static final String GOBBLIN_SPEC_STORE_MONITOR_UNEXPECTED_ERRORS = ServiceMetricNames.GOBBLIN_SERVICE_PREFIX + "gobblin.specStoreMonitor.unexpected.errors";
  public static final String GOBBLIN_SPEC_STORE_MESSAGE_PROCESSED= ServiceMetricNames.GOBBLIN_SERVICE_PREFIX + "gobblin.specStoreMonitor.message.processed";
  public static final String GOBBLIN_DAG_ACTION_STORE_MONITOR_KILLS_INVOKED = ServiceMetricNames.GOBBLIN_SERVICE_PREFIX + "gobblin.dagActionStore.kills.invoked";
  public static final String GOBBLIN_DAG_ACTION_STORE_MONITOR_MESSAGE_PROCESSED= ServiceMetricNames.GOBBLIN_SERVICE_PREFIX + "gobblin.dagActionStoreMonitor.message.processed";
  public static final String GOBBLIN_DAG_ACTION_STORE_MONITOR_RESUMES_INVOKED = ServiceMetricNames.GOBBLIN_SERVICE_PREFIX + "gobblin.dagActionStore.resumes.invoked";
  public static final String GOBBLIN_DAG_ACTION_STORE_MONITOR_UNEXPECTED_ERRORS = ServiceMetricNames.GOBBLIN_SERVICE_PREFIX + "gobblin.dagActionStore.unexpected.errors";

  public static final String GOBBLIN_MYSQL_QUOTA_MANAGER_UNEXPECTED_ERRORS = ServiceMetricNames.GOBBLIN_SERVICE_PREFIX + "gobblin.mysql.quota.manager.unexpected.errors";
  public static final String GOBBLIN_MYSQL_QUOTA_MANAGER_QUOTA_REQUESTS_EXCEEDED = ServiceMetricNames.GOBBLIN_SERVICE_PREFIX + "gobblin.mysql.quota.manager.quotaRequests.exceeded";
  public static final String GOBBLIN_MYSQL_QUOTA_MANAGER_TIME_TO_CHECK_QUOTA = ServiceMetricNames.GOBBLIN_SERVICE_PREFIX + "gobblin.mysql.quota.manager.time.to.check.quota";

  // Metadata keys
  public static final String TOPIC = "topic";
  public static final String GROUP_ID = "groupId";
  public static final String SCHEMA = "schema";
}
