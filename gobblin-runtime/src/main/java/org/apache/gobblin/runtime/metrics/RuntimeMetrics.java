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

package gobblin.runtime.metrics;

/**
 * Constants used for naming {@link gobblin.metrics.Metric}s and metric metadata in gobblin-runtime.
 */
public class RuntimeMetrics {

  // Metric names
  public static final String GOBBLIN_KAFKA_HIGH_LEVEL_CONSUMER_MESSAGES_READ =
      "gobblin.kafka.highLevelConsumer.messagesRead";
  public static final String GOBBLIN_JOB_MONITOR_KAFKA_NEW_SPECS = "gobblin.jobMonitor.kafka.newSpecs";
  public static final String GOBBLIN_JOB_MONITOR_KAFKA_REMOVED_SPECS = "gobblin.jobMonitor.kafka.removedSpecs";
  public static final String GOBBLIN_JOB_MONITOR_SLAEVENT_REJECTEDEVENTS = "gobblin.jobMonitor.slaevent.rejectedevents";
  public static final String GOBBLIN_JOB_MONITOR_KAFKA_MESSAGE_PARSE_FAILURES =
      "gobblin.jobMonitor.kafka.messageParseFailures";

  // Metadata keys
  public static final String TOPIC = "topic";
  public static final String GROUP_ID = "groupId";
  public static final String SCHEMA = "schema";
}
