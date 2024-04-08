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

package org.apache.gobblin.temporal.ddm.activity.impl;

import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import io.temporal.failure.ApplicationFailure;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.broker.gobblin_scopes.GobblinScopeTypes;
import org.apache.gobblin.broker.iface.SharedResourcesBroker;
import org.apache.gobblin.commit.DeliverySemantics;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.metastore.StateStore;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.runtime.DatasetTaskSummary;
import org.apache.gobblin.runtime.JobContext;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.runtime.SafeDatasetCommit;
import org.apache.gobblin.runtime.TaskState;
import org.apache.gobblin.runtime.TaskStateCollectorService;
import org.apache.gobblin.runtime.util.GsonUtils;
import org.apache.gobblin.source.extractor.JobCommitPolicy;
import org.apache.gobblin.temporal.ddm.activity.CommitActivity;
import org.apache.gobblin.temporal.ddm.util.JobStateUtils;
import org.apache.gobblin.temporal.ddm.work.WUProcessingSpec;
import org.apache.gobblin.temporal.ddm.work.assistance.Help;
import org.apache.gobblin.temporal.workflows.metrics.TemporalEventTimer;
import org.apache.gobblin.util.Either;
import org.apache.gobblin.util.ExecutorsUtils;
import org.apache.gobblin.util.PropertiesUtils;
import org.apache.gobblin.util.executors.IteratorExecutor;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

@Slf4j
public class CommitActivityImpl implements CommitActivity {

  static int DEFAULT_NUM_DESERIALIZATION_THREADS = 10;
  static int DEFAULT_NUM_COMMIT_THREADS = 1;
  static String UNDEFINED_JOB_NAME = "<job_name_stub>";

  @Override
  public int commit(WUProcessingSpec workSpec) {
    // TODO: Make this configurable
    int numDeserializationThreads = DEFAULT_NUM_DESERIALIZATION_THREADS;
    Optional<String> jobNameOpt = Optional.empty();
    try {
      FileSystem fs = Help.loadFileSystem(workSpec);
      JobState jobState = Help.loadJobState(workSpec, fs);
      jobNameOpt = Optional.ofNullable(jobState.getJobName());
      SharedResourcesBroker<GobblinScopeTypes> instanceBroker = JobStateUtils.getSharedResourcesBroker(jobState);
      JobContext globalGobblinContext = new JobContext(jobState.getProperties(), log, instanceBroker, null);
      // TODO: Task state dir is a stub with the assumption it is always colocated with the workunits dir (as in the case of MR which generates workunits)
      Path jobIdParent = new Path(workSpec.getWorkUnitsDir()).getParent();
      Path jobOutputPath = new Path(new Path(jobIdParent, "output"), jobIdParent.getName());
      log.info("Output path at: " + jobOutputPath + " with fs at " + fs.getUri());
      StateStore<TaskState> taskStateStore = Help.openTaskStateStore(workSpec, fs);
      Optional<Queue<TaskState>> taskStateQueueOpt =
              TaskStateCollectorService.deserializeTaskStatesFromFolder(taskStateStore, jobOutputPath.getName(), numDeserializationThreads);
      if (!taskStateQueueOpt.isPresent()) {
        log.error("No task states found at " + jobOutputPath);
        return 0;
      }
      Queue<TaskState> taskStateQueue = taskStateQueueOpt.get();
      Map<String, JobState.DatasetState> datasetStatesByUrns = createDatasetStatesByUrns(ImmutableList.copyOf(taskStateQueue));
      commitTaskStates(jobState, datasetStatesByUrns, globalGobblinContext, jobNameOpt);
      List<DatasetTaskSummary> datasetTaskSummaries = generateDatasetTaskSummaries(datasetStatesByUrns, globalGobblinContext, workSpec.getEventSubmitterContext().create());
      // Submit event that summarizes work done
      TemporalEventTimer.Factory timerFactory = new TemporalEventTimer.Factory(workSpec.getEventSubmitterContext());
      TemporalEventTimer eventTimer = timerFactory.create(TimingEvent.LauncherTimings.JOB_SUMMARY);
      eventTimer.addMetadata(TimingEvent.DATASET_TASK_SUMMARIES, GsonUtils.GSON_WITH_DATE_HANDLING.toJson(datasetTaskSummaries));
      eventTimer.stop();
      return taskStateQueue.size();
    } catch (Exception e) {
      //TODO: IMPROVE GRANULARITY OF RETRIES
      throw ApplicationFailure.newNonRetryableFailureWithCause(
          String.format("Failed to commit dataset state for some dataset(s) of job %s", jobNameOpt.orElse(UNDEFINED_JOB_NAME)),
          IOException.class.toString(),
          new IOException(e),
          null
      );
    }
  }

  /**
   * Commit task states to the dataset state store.
   * @param jobState
   * @param datasetStatesByUrns
   * @param jobContext
   * @throws IOException
   */
  private void commitTaskStates(State jobState, Map<String, JobState.DatasetState> datasetStatesByUrns,
      JobContext jobContext, Optional<String> jobNameOpt) throws IOException {
    final boolean shouldCommitDataInJob = JobContext.shouldCommitDataInJob(jobState);
    final DeliverySemantics deliverySemantics = DeliverySemantics.AT_LEAST_ONCE;
    //TODO: Make this configurable
    final int numCommitThreads = DEFAULT_NUM_COMMIT_THREADS;
    if (!shouldCommitDataInJob) {
      if (Strings.isNullOrEmpty(jobState.getProp(ConfigurationKeys.JOB_DATA_PUBLISHER_TYPE))) {
        log.warn("No data publisher is configured for this job. This can lead to non-atomic commit behavior.");
      }
      log.info("Job will not commit data since data are committed by tasks.");
    }

    try {
      if (!datasetStatesByUrns.isEmpty()) {
        log.info("Persisting {} dataset urns.", datasetStatesByUrns.size());
      }

      List<Either<Void, ExecutionException>> result = new IteratorExecutor<>(Iterables
          .transform(datasetStatesByUrns.entrySet(),
              new Function<Map.Entry<String, JobState.DatasetState>, Callable<Void>>() {
                @Nullable
                @Override
                public Callable<Void> apply(final Map.Entry<String, JobState.DatasetState> entry) {
                  return new SafeDatasetCommit(shouldCommitDataInJob, false, deliverySemantics, entry.getKey(),
                      entry.getValue(), false, jobContext);
                }
              }).iterator(), numCommitThreads,
          // TODO: Rewrite executorUtils to use java util optional
          ExecutorsUtils.newThreadFactory(com.google.common.base.Optional.of(log), com.google.common.base.Optional.of("Commit-thread-%d")))
          .executeAndGetResults();

      IteratorExecutor.logFailures(result, null, 10);

      Set<String> failedDatasetUrns = datasetStatesByUrns.values().stream()
          .map(JobState.DatasetState::getDatasetUrn)
          .collect(Collectors.toCollection(HashSet::new));

      if (!failedDatasetUrns.isEmpty()) {
        String allFailedDatasets = String.join(", ", failedDatasetUrns);
        log.error("Failed to commit dataset state for dataset(s) {}" + allFailedDatasets);
        throw new IOException("Failed to commit dataset state for " + allFailedDatasets);
      }
      if (!IteratorExecutor.verifyAllSuccessful(result)) {
        // TODO: propagate cause of failure and determine whether or not this is retryable to throw a non-retryable failure exception
        throw new IOException("Failed to commit dataset state for some dataset(s) of job " + jobNameOpt.orElse(UNDEFINED_JOB_NAME));
      }
    } catch (InterruptedException exc) {
      throw new IOException(exc);
    }
  }

  /**
   * Organize task states by dataset urns.
   * @param taskStates
   * @return A map of dataset urns to dataset task states.
   */
  public static Map<String, JobState.DatasetState> createDatasetStatesByUrns(Collection<TaskState> taskStates) {
    Map<String, JobState.DatasetState> datasetStatesByUrns = Maps.newHashMap();

    //TODO: handle skipped tasks?
    for (TaskState taskState : taskStates) {
      String datasetUrn = createDatasetUrn(datasetStatesByUrns, taskState);
      datasetStatesByUrns.get(datasetUrn).incrementTaskCount();
      datasetStatesByUrns.get(datasetUrn).addTaskState(taskState);
    }

    return datasetStatesByUrns;
  }

  public List<DatasetTaskSummary> generateDatasetTaskSummaries(Map<String, JobState.DatasetState> datasetStatesByUrns, JobContext jobContext, EventSubmitter eventSubmitter) {
    List<DatasetTaskSummary> datasetTaskSummaries = new ArrayList<>();
    // Only process successful datasets unless configuration to process failed datasets is set
    boolean processFailedTasks =
        PropertiesUtils.getPropAsBoolean(jobContext.getJobState().getProperties(), ConfigurationKeys.WRITER_COUNT_METRICS_FROM_FAILED_TASKS,
            "false");
    for (JobState.DatasetState datasetState : datasetStatesByUrns.values()) {
      if (datasetState.getState() == JobState.RunningState.COMMITTED || (datasetState.getState() == JobState.RunningState.FAILED
          && jobContext.getJobCommitPolicy() == JobCommitPolicy.COMMIT_SUCCESSFUL_TASKS)) {
        long totalBytesWritten = 0;
        long totalRecordsWritten = 0;
        for (TaskState taskState : datasetState.getTaskStates()) {
          // Certain writers may omit these metrics e.g. CompactionLauncherWriter
          if ((taskState.getWorkingState() == WorkUnitState.WorkingState.COMMITTED || processFailedTasks)) {
            totalBytesWritten += taskState.getPropAsLong(ConfigurationKeys.WRITER_BYTES_WRITTEN, 0);
            totalRecordsWritten += taskState.getPropAsLong(ConfigurationKeys.WRITER_RECORDS_WRITTEN, 0);
          }
        }
        log.info(String.format("DatasetMetrics for '%s' - (records: %d; bytes: %d)", datasetState.getDatasetUrn(),
            totalRecordsWritten, totalBytesWritten));
        datasetTaskSummaries.add(
            new DatasetTaskSummary(datasetState.getDatasetUrn(), totalRecordsWritten, totalBytesWritten,
                datasetState.getState() == JobState.RunningState.COMMITTED));
      } else if (datasetState.getState() == JobState.RunningState.FAILED) {
        // Check if config is turned on for submitting writer metrics on failure due to non-atomic write semantics
        if (jobContext.getJobCommitPolicy() == JobCommitPolicy.COMMIT_ON_FULL_SUCCESS) {
          log.info("Due to task failure, will report that no records or bytes were written for " + datasetState.getDatasetUrn());
          datasetTaskSummaries.add(new DatasetTaskSummary(datasetState.getDatasetUrn(), 0, 0, false));
        }
      }
    }
    return datasetTaskSummaries;
  }

  private static String createDatasetUrn(Map<String, JobState.DatasetState> datasetStatesByUrns, TaskState taskState) {
    String datasetUrn = taskState.getProp(ConfigurationKeys.DATASET_URN_KEY, ConfigurationKeys.DEFAULT_DATASET_URN);
    if (!datasetStatesByUrns.containsKey(datasetUrn)) {
      JobState.DatasetState datasetState = new JobState.DatasetState();
      datasetState.setDatasetUrn(datasetUrn);
      datasetStatesByUrns.put(datasetUrn, datasetState);
    }
    return datasetUrn;
  }
}
