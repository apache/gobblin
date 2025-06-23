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

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import lombok.extern.slf4j.Slf4j;

import com.google.api.client.util.Lists;
import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.io.Closer;

import io.temporal.activity.Activity;
import io.temporal.activity.ActivityExecutionContext;
import io.temporal.failure.ApplicationFailure;

import org.apache.gobblin.broker.gobblin_scopes.GobblinScopeTypes;
import org.apache.gobblin.broker.iface.SharedResourcesBroker;
import org.apache.gobblin.commit.DeliverySemantics;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.initializer.ConverterInitializerFactory;
import org.apache.gobblin.initializer.Initializer;
import org.apache.gobblin.metastore.StateStore;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.runtime.JobContext;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.runtime.SafeDatasetCommit;
import org.apache.gobblin.runtime.TaskState;
import org.apache.gobblin.runtime.TaskStateCollectorService;
import org.apache.gobblin.runtime.troubleshooter.AutomaticTroubleshooter;
import org.apache.gobblin.runtime.troubleshooter.AutomaticTroubleshooterFactory;
import org.apache.gobblin.source.extractor.JobCommitPolicy;
import org.apache.gobblin.source.workunit.BasicWorkUnitStream;
import org.apache.gobblin.source.workunit.WorkUnitStream;
import org.apache.gobblin.temporal.ddm.activity.CommitActivity;
import org.apache.gobblin.temporal.ddm.util.JobStateUtils;
import org.apache.gobblin.temporal.ddm.work.CommitStats;
import org.apache.gobblin.temporal.ddm.work.DatasetStats;
import org.apache.gobblin.temporal.ddm.work.WUProcessingSpec;
import org.apache.gobblin.temporal.ddm.work.assistance.Help;
import org.apache.gobblin.temporal.exception.FailedDatasetUrnsException;
import org.apache.gobblin.util.Either;
import org.apache.gobblin.util.ExecutorsUtils;
import org.apache.gobblin.util.PropertiesUtils;
import org.apache.gobblin.util.executors.IteratorExecutor;
import org.apache.gobblin.writer.initializer.WriterInitializerFactory;


@Slf4j
public class CommitActivityImpl implements CommitActivity {

  static int DEFAULT_NUM_DESERIALIZATION_THREADS = 10;
  static int DEFAULT_NUM_COMMIT_THREADS = 1;
  static String UNDEFINED_JOB_NAME = "<<UNKNOWN JOB NAME>>";

  @Override
  public CommitStats commit(WUProcessingSpec workSpec) {
    ActivityExecutionContext activityExecutionContext = Activity.getExecutionContext();
    ScheduledExecutorService heartBeatExecutor = Executors.newSingleThreadScheduledExecutor(
        ExecutorsUtils.newThreadFactory(com.google.common.base.Optional.of(log),
            com.google.common.base.Optional.of("CommitActivityHeartBeatExecutor")));
    // TODO: Make this configurable
    int numDeserializationThreads = DEFAULT_NUM_DESERIALIZATION_THREADS;
    Optional<String> optJobName = Optional.empty();
    AutomaticTroubleshooter troubleshooter = null;
    try (FileSystem fs = Help.loadFileSystem(workSpec)) {
      JobState jobState = Help.loadJobState(workSpec, fs);

      int heartBeatInterval = JobStateUtils.getHeartBeatInterval(jobState);
      heartBeatExecutor.scheduleAtFixedRate(() -> activityExecutionContext.heartbeat("Running Commit Activity"),
          heartBeatInterval, heartBeatInterval, TimeUnit.MINUTES);

      optJobName = Optional.ofNullable(jobState.getJobName());
      SharedResourcesBroker<GobblinScopeTypes> instanceBroker = JobStateUtils.getSharedResourcesBroker(jobState);
      troubleshooter = AutomaticTroubleshooterFactory.createForJob(jobState.getProperties());
      troubleshooter.start();
      List<TaskState> taskStates = loadTaskStates(workSpec, fs, jobState, numDeserializationThreads);
      if (taskStates.isEmpty()) {
        return CommitStats.createEmpty();
      }

      JobContext jobContext = new JobContext(jobState.getProperties(), log, instanceBroker, troubleshooter.getIssueRepository());
      Map<String, JobState.DatasetState> datasetStatesByUrns = jobState.calculateDatasetStatesByUrns(ImmutableList.copyOf(taskStates), Lists.newArrayList());
      TaskState firstTaskState = taskStates.get(0);
      log.info("TaskState (commit) [{}] (**first of {}**): {}", firstTaskState.getTaskId(), taskStates.size(), firstTaskState.toJsonString(true));
      Optional<FailedDatasetUrnsException> optFailure = Optional.empty();
      try {
        commitTaskStates(jobState, datasetStatesByUrns, jobContext);
      } catch (FailedDatasetUrnsException exception) {
        log.warn("Some datasets failed to be committed, proceeding with publishing commit step", exception);
        optFailure = Optional.of(exception);
      } finally {
        // if Work Discovery transmitted any writer/converter `Initializer.AfterInitializeMemento`s within `jobState`, deserialize them now to
        // `.recall()` and "re-initialize" equivalent writer and/or converter(s) `Initializer`s, to complete their `.close()`
        // NOTE: the "revived" `Initializer`s are constructed with empty placeholder WUs
        Closer closer = Closer.create(); // (purely to suppress exceptions)
        Optional.ofNullable(jobState.getProp(ConfigurationKeys.WRITER_INITIALIZER_SERIALIZED_MEMENTO_KEY)).map(mementoProp ->
            Initializer.AfterInitializeMemento.deserialize(mementoProp)
        ).ifPresent(memento ->
            closer.register(WriterInitializerFactory.newInstace(jobState, createEmptyWorkUnitStream())).recall(memento)
        );
        Optional.ofNullable(jobState.getProp(ConfigurationKeys.CONVERTER_INITIALIZERS_SERIALIZED_MEMENTOS_KEY)).map(mementoProp ->
            Initializer.AfterInitializeMemento.deserialize(mementoProp)
        ).ifPresent(memento ->
            closer.register(ConverterInitializerFactory.newInstance(jobState, createEmptyWorkUnitStream())).recall(memento)
        );
        closer.close();
      }

      boolean shouldIncludeFailedTasks = PropertiesUtils.getPropAsBoolean(jobState.getProperties(), ConfigurationKeys.WRITER_COUNT_METRICS_FROM_FAILED_TASKS, "false");

      Map<String, DatasetStats> datasetTaskSummaries = summarizeDatasetOutcomes(datasetStatesByUrns, jobContext.getJobCommitPolicy(), shouldIncludeFailedTasks);
      return new CommitStats(
          datasetTaskSummaries,
          datasetTaskSummaries.values().stream().mapToInt(DatasetStats::getNumCommittedWorkunits).sum(),
          optFailure
      );
    } catch (Exception e) {
      //TODO: IMPROVE GRANULARITY OF RETRIES
      throw ApplicationFailure.newNonRetryableFailureWithCause(
          String.format("Failed to commit dataset state for some dataset(s) of job %s", optJobName.orElse(UNDEFINED_JOB_NAME)),
          IOException.class.toString(),
          new IOException(e)
      );
    } finally {
      String errCorrelator = String.format("Commit [%s]", calcCommitId(workSpec));
      EventSubmitter eventSubmitter = workSpec.getEventSubmitterContext().create();
      Help.finalizeTroubleshooting(troubleshooter, eventSubmitter, log, errCorrelator);
      ExecutorsUtils.shutdownExecutorService(heartBeatExecutor, com.google.common.base.Optional.of(log));
    }
  }

  /**
   * Commit task states to the dataset state store.
   * @param jobState
   * @param datasetStatesByUrns
   * @param jobContext
   * @throws IOException
   */
  private void commitTaskStates(JobState jobState, Map<String, JobState.DatasetState> datasetStatesByUrns, JobContext jobContext) throws IOException {
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
          .filter(datasetState -> datasetState.getState() == JobState.RunningState.FAILED)
          .map(JobState.DatasetState::getDatasetUrn)
          .collect(Collectors.toCollection(HashSet::new));

      if (!failedDatasetUrns.isEmpty()) {
        String allFailedDatasets = String.join(", ", failedDatasetUrns);
        log.error("Failed to commit dataset state for dataset(s) {}", allFailedDatasets);
        throw new FailedDatasetUrnsException(failedDatasetUrns);
      }
      if (!IteratorExecutor.verifyAllSuccessful(result)) {
        // TODO: propagate cause of failure and determine whether or not this is retryable to throw a non-retryable failure exception
        String jobName = jobState.getProp(ConfigurationKeys.JOB_NAME_KEY, UNDEFINED_JOB_NAME);
        throw new IOException("Failed to commit dataset state for some dataset(s) of job " + jobName);
      }
    } catch (InterruptedException exc) {
      throw new IOException(exc);
    }
  }

  /** @return {@link TaskState}s loaded from the {@link StateStore<TaskState>} indicated by the {@link WUProcessingSpec} and {@link FileSystem} */
  private List<TaskState> loadTaskStates(WUProcessingSpec workSpec, FileSystem fs, JobState jobState, int numThreads) throws IOException {
    // TODO - decide whether to replace this method by adapting TaskStateCollectorService::collectOutputTaskStates (whence much of this code was drawn)
    StateStore<TaskState> taskStateStore = Help.openTaskStateStore(workSpec, fs);
    // NOTE: TaskState dir is assumed to be a sibling to the workunits dir (following conventions of `MRJobLauncher`)
    String jobIdPathName = new Path(workSpec.getWorkUnitsDir()).getParent().getName();
    log.info("TaskStateStore path (name component): '{}' (fs: '{}')", jobIdPathName, fs.getUri());
    Optional<Queue<TaskState>> taskStateQueueOpt = TaskStateCollectorService.deserializeTaskStatesFromFolder(taskStateStore, jobIdPathName, numThreads);
    return taskStateQueueOpt.map(taskStateQueue ->
        taskStateQueue.stream().peek(taskState ->
                // CRITICAL: although some `WorkUnit`s, like those created by `CopySource::FileSetWorkUnitGenerator` for each `CopyEntity`
                // already themselves contain every prop of their `JobState`, not all do.
                // `TaskState extends WorkUnit` serialization will include its constituent `WorkUnit`, but not the constituent `JobState`.
                // given some `JobState` props may be essential for commit/publish, deserialization must re-associate each `TaskState` w/ `JobState`
                taskState.setJobState(jobState)
            // TODO - decide whether something akin necessary to streamline cumulative in-memory size of all issues: consumeTaskIssues(taskState);
        ).collect(Collectors.toList())
    ).orElseGet(() -> {
      log.error("TaskStateStore successfully opened, but no task states found under (name) '{}'", jobIdPathName);
      return Lists.newArrayList();
    });
  }

  private Map<String, DatasetStats> summarizeDatasetOutcomes(Map<String, JobState.DatasetState> datasetStatesByUrns, JobCommitPolicy commitPolicy, boolean shouldIncludeFailedTasks) {
    Map<String, DatasetStats> datasetTaskStats = new HashMap<>();
    // Only process successful datasets unless configuration to process failed datasets is set
    for (JobState.DatasetState datasetState : datasetStatesByUrns.values()) {
      if (datasetState.getState() == JobState.RunningState.COMMITTED || (datasetState.getState() == JobState.RunningState.FAILED
          && commitPolicy.isAllowPartialCommit())) {
        long totalBytesWritten = 0;
        long totalRecordsWritten = 0;
        int totalCommittedTasks = 0;
        for (TaskState taskState : datasetState.getTaskStates()) {
          // Certain writers may omit these metrics e.g. CompactionLauncherWriter
          if (taskState.getWorkingState() == WorkUnitState.WorkingState.COMMITTED || shouldIncludeFailedTasks) {
            if (taskState.getWorkingState() == WorkUnitState.WorkingState.COMMITTED) {
              totalCommittedTasks++;
            }
            totalBytesWritten += taskState.getPropAsLong(ConfigurationKeys.WRITER_BYTES_WRITTEN, 0);
            totalRecordsWritten += taskState.getPropAsLong(ConfigurationKeys.WRITER_RECORDS_WRITTEN, 0);
          }
        }
        log.info(String.format("DatasetMetrics for '%s' - (records: %d; bytes: %d)", datasetState.getDatasetUrn(),
            totalRecordsWritten, totalBytesWritten));
        datasetTaskStats.put(datasetState.getDatasetUrn(), new DatasetStats(totalRecordsWritten, totalBytesWritten, true, totalCommittedTasks, datasetState.getDataQualityStatus().name()));
      } else if (datasetState.getState() == JobState.RunningState.FAILED && commitPolicy == JobCommitPolicy.COMMIT_ON_FULL_SUCCESS) {
        // Check if config is turned on for submitting writer metrics on failure due to non-atomic write semantics
        log.info("Due to task failure, will report that no records or bytes were written for " + datasetState.getDatasetUrn());
        datasetTaskStats.put(datasetState.getDatasetUrn(), new DatasetStats( 0, 0, false, 0, datasetState.getDataQualityStatus().name()));
      }
    }
    return datasetTaskStats;
  }

  /** @return id/correlator for this particular commit activity */
  private static String calcCommitId(WUProcessingSpec workSpec) {
    return new Path(workSpec.getWorkUnitsDir()).getParent().getName();
  }

  private static WorkUnitStream createEmptyWorkUnitStream() {
    return new BasicWorkUnitStream.Builder(Lists.newArrayList()).build();
  }
}
