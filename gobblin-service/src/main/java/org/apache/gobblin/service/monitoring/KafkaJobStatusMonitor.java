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

package org.apache.gobblin.service.monitoring;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import avro.shaded.com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.kafka.client.DecodeableKafkaRecord;
import org.apache.gobblin.metastore.FileContextBasedFsStateStore;
import org.apache.gobblin.metastore.FileContextBasedFsStateStoreFactory;
import org.apache.gobblin.metastore.StateStore;
import org.apache.gobblin.metrics.ServiceMetricNames;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.runtime.TaskContext;
import org.apache.gobblin.runtime.kafka.HighLevelConsumer;
import org.apache.gobblin.runtime.retention.DatasetCleanerTask;
import org.apache.gobblin.service.ExecutionStatus;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.util.ConfigUtils;


/**
 * A Kafka monitor that tracks {@link org.apache.gobblin.metrics.GobblinTrackingEvent}s reporting statuses of
 * running jobs. The job statuses are stored as {@link org.apache.gobblin.configuration.State} objects in
 * a {@link FileContextBasedFsStateStore}.
 */
@Slf4j
public abstract class KafkaJobStatusMonitor extends HighLevelConsumer<byte[], byte[]> {
  static final String JOB_STATUS_MONITOR_PREFIX = "jobStatusMonitor";
  //We use table suffix that is different from the Gobblin job state store suffix of jst to avoid confusion.
  //gst refers to the state store suffix for GaaS-orchestrated Gobblin jobs.
  public static final String STATE_STORE_TABLE_SUFFIX = "gst";
  public static final String STATE_STORE_KEY_SEPARATION_CHARACTER = ".";
  public static final String GET_AND_SET_JOB_STATUS = MetricRegistry.name(ServiceMetricNames.GOBBLIN_SERVICE_PREFIX,
      JOB_STATUS_MONITOR_PREFIX,  "getAndSetJobStatus");

  static final String JOB_STATUS_MONITOR_TOPIC_KEY = "topic";
  static final String JOB_STATUS_MONITOR_NUM_THREADS_KEY = "numThreads";
  static final String JOB_STATUS_MONITOR_CLASS_KEY = "class";
  static final String DEFAULT_JOB_STATUS_MONITOR_CLASS = KafkaAvroJobStatusMonitor.class.getName();

  private static final String KAFKA_AUTO_OFFSET_RESET_KEY = "auto.offset.reset";
  private static final String KAFKA_AUTO_OFFSET_RESET_SMALLEST = "smallest";

  @Getter
  private final StateStore<org.apache.gobblin.configuration.State> stateStore;
  private final ScheduledExecutorService scheduledExecutorService;
  private static final Config DEFAULTS = ConfigFactory.parseMap(ImmutableMap.of(
      KAFKA_AUTO_OFFSET_RESET_KEY, KAFKA_AUTO_OFFSET_RESET_SMALLEST));

  private static final List<ExecutionStatus> ORDERED_EXECUTION_STATUSES = ImmutableList
      .of(ExecutionStatus.COMPILED, ExecutionStatus.PENDING, ExecutionStatus.PENDING_RETRY,
          ExecutionStatus.ORCHESTRATED, ExecutionStatus.RUNNING, ExecutionStatus.COMPLETE,
          ExecutionStatus.FAILED, ExecutionStatus.CANCELLED);

  public KafkaJobStatusMonitor(String topic, Config config, int numThreads)
      throws ReflectiveOperationException {
    super(topic, config.withFallback(DEFAULTS), numThreads);
    String stateStoreFactoryClass = ConfigUtils.getString(config, ConfigurationKeys.STATE_STORE_FACTORY_CLASS_KEY, FileContextBasedFsStateStoreFactory.class.getName());

    this.stateStore =
        ((StateStore.Factory) Class.forName(stateStoreFactoryClass).newInstance()).createStateStore(config, org.apache.gobblin.configuration.State.class);
    this.scheduledExecutorService = Executors.newScheduledThreadPool(1);
  }

  @Override
  protected void startUp() {
    super.startUp();
    log.info("Scheduling state store cleaner..");
    org.apache.gobblin.configuration.State state = new org.apache.gobblin.configuration.State(ConfigUtils.configToProperties(this.config));
    state.setProp(ConfigurationKeys.JOB_ID_KEY, "GobblinServiceJobStatusCleanerJob");
    state.setProp(ConfigurationKeys.TASK_ID_KEY, "GobblinServiceJobStatusCleanerTask");

    TaskContext taskContext = new TaskContext(new WorkUnitState(WorkUnit.createEmpty(), state));
    DatasetCleanerTask cleanerTask = new DatasetCleanerTask(taskContext);
    scheduledExecutorService.scheduleAtFixedRate(cleanerTask, 300L, 86400L, TimeUnit.SECONDS);
  }

  @Override
  public void shutDown() {
     super.shutDown();
     this.scheduledExecutorService.shutdown();
     try {
       this.scheduledExecutorService.awaitTermination(30, TimeUnit.SECONDS);
     } catch (InterruptedException e) {
       log.error("Exception encountered when shutting down state store cleaner", e);
     }
  }

  @Override
  protected void createMetrics() {
    super.createMetrics();
  }

  @Override
  protected void processMessage(DecodeableKafkaRecord<byte[],byte[]> message) {
    try {
      org.apache.gobblin.configuration.State jobStatus = parseJobStatus(message);
      if (jobStatus != null) {
        try(Timer.Context context = getMetricContext().timer(GET_AND_SET_JOB_STATUS).time()) {
          addJobStatusToStateStore(jobStatus, this.stateStore);
        }
      }
    } catch (IOException ioe) {
      // Throw RuntimeException to avoid advancing kafka offsets without updating state store
      throw new RuntimeException("Failed to add job status to state store for kafka offset " + message.getOffset(), ioe);
    }
  }

  /**
   * Persist job status to the underlying {@link StateStore}.
   * It fills missing fields in job status and also merge the fields with the
   * existing job status in the state store. Merging is required because we
   * do not want to lose the information sent by other GobblinTrackingEvents.
   * @param jobStatus
   * @throws IOException
   */
  @VisibleForTesting
  static void addJobStatusToStateStore(org.apache.gobblin.configuration.State jobStatus, StateStore stateStore)
      throws IOException {
    if (!jobStatus.contains(TimingEvent.FlowEventConstants.JOB_NAME_FIELD)) {
      jobStatus.setProp(TimingEvent.FlowEventConstants.JOB_NAME_FIELD, JobStatusRetriever.NA_KEY);
    }
    if (!jobStatus.contains(TimingEvent.FlowEventConstants.JOB_GROUP_FIELD)) {
      jobStatus.setProp(TimingEvent.FlowEventConstants.JOB_GROUP_FIELD, JobStatusRetriever.NA_KEY);
    }

    String flowName = jobStatus.getProp(TimingEvent.FlowEventConstants.FLOW_NAME_FIELD);
    String flowGroup = jobStatus.getProp(TimingEvent.FlowEventConstants.FLOW_GROUP_FIELD);
    String flowExecutionId = jobStatus.getProp(TimingEvent.FlowEventConstants.FLOW_EXECUTION_ID_FIELD);
    String jobName = jobStatus.getProp(TimingEvent.FlowEventConstants.JOB_NAME_FIELD);
    String jobGroup = jobStatus.getProp(TimingEvent.FlowEventConstants.JOB_GROUP_FIELD);
    String storeName = jobStatusStoreName(flowGroup, flowName);
    String tableName = jobStatusTableName(flowExecutionId, jobGroup, jobName);

    List<org.apache.gobblin.configuration.State> states = stateStore.getAll(storeName, tableName);
    if (states.size() > 0) {
      String previousStatus = states.get(states.size() - 1).getProp(JobStatusRetriever.EVENT_NAME_FIELD);
      String currentStatus = jobStatus.getProp(JobStatusRetriever.EVENT_NAME_FIELD);

      if (previousStatus != null && currentStatus != null && ORDERED_EXECUTION_STATUSES.indexOf(ExecutionStatus.valueOf(currentStatus))
          < ORDERED_EXECUTION_STATUSES.indexOf(ExecutionStatus.valueOf(previousStatus))) {
        log.warn(String.format("Received status %s when status is already %s for flow (%s, %s, %s), job (%s, %s)",
            currentStatus, previousStatus, flowGroup, flowName, flowExecutionId, jobGroup, jobName));
        jobStatus = mergeState(states.get(states.size() - 1), jobStatus);
      } else {
        jobStatus = mergeState(jobStatus, states.get(states.size() - 1));
      }
    }

    modifyStateIfRetryRequired(jobStatus);

    stateStore.put(storeName, tableName, jobStatus);
  }

  private static void modifyStateIfRetryRequired(org.apache.gobblin.configuration.State state) {
    int maxAttempts = state.getPropAsInt(TimingEvent.FlowEventConstants.MAX_ATTEMPTS_FIELD, 1);
    int currentAttempts = state.getPropAsInt(TimingEvent.FlowEventConstants.CURRENT_ATTEMPTS_FIELD, 1);
    if (state.getProp(JobStatusRetriever.EVENT_NAME_FIELD).equals(ExecutionStatus.FAILED.name()) && currentAttempts < maxAttempts) {
      state.setProp(TimingEvent.FlowEventConstants.SHOULD_RETRY_FIELD, true);
      state.setProp(JobStatusRetriever.EVENT_NAME_FIELD, ExecutionStatus.PENDING_RETRY.name());
      state.removeProp(TimingEvent.JOB_END_TIME);
    }
  }

  /**
   * Merge states based on precedence defined by {@link #ORDERED_EXECUTION_STATUSES}.
   * The state instance in the 1st argument reflects the more recent state of a job
   * (and is thus, given higher priority) compared to the 2nd argument.
   * @param state higher priority state
   * @param fallbackState lower priority state
   * @return merged state
   */
  private static org.apache.gobblin.configuration.State mergeState(org.apache.gobblin.configuration.State state,
      org.apache.gobblin.configuration.State fallbackState) {
    Properties mergedState = new Properties();

    mergedState.putAll(fallbackState.getProperties());
    mergedState.putAll(state.getProperties());

    return new org.apache.gobblin.configuration.State(mergedState);
  }

  public static String jobStatusTableName(String flowExecutionId, String jobGroup, String jobName) {
    return Joiner.on(STATE_STORE_KEY_SEPARATION_CHARACTER).join(flowExecutionId, jobGroup, jobName, STATE_STORE_TABLE_SUFFIX);
  }

  public static String jobStatusTableName(long flowExecutionId, String jobGroup, String jobName) {
    return jobStatusTableName(String.valueOf(flowExecutionId), jobGroup, jobName);
  }

  public static String jobStatusStoreName(String flowGroup, String flowName) {
    return Joiner.on(STATE_STORE_KEY_SEPARATION_CHARACTER).join(flowGroup, flowName);
  }

  public static long getExecutionIdFromTableName(String tableName) {
    return Long.parseLong(Splitter.on(STATE_STORE_KEY_SEPARATION_CHARACTER).splitToList(tableName).get(0));
  }

  public abstract org.apache.gobblin.configuration.State parseJobStatus(DecodeableKafkaRecord<byte[],byte[]> message);

}
