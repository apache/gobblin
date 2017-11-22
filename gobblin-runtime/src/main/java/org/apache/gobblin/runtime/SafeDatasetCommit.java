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

package org.apache.gobblin.runtime;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.commons.lang.StringUtils;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Closer;

import org.apache.gobblin.commit.CommitSequence;
import org.apache.gobblin.commit.CommitStep;
import org.apache.gobblin.commit.DeliverySemantics;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.event.lineage.LineageEventBuilder;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.event.FailureEventBuilder;
import org.apache.gobblin.metrics.event.lineage.LineageInfo;
import org.apache.gobblin.publisher.CommitSequencePublisher;
import org.apache.gobblin.publisher.DataPublisher;
import org.apache.gobblin.publisher.UnpublishedHandling;
import org.apache.gobblin.runtime.commit.DatasetStateCommitStep;
import org.apache.gobblin.runtime.task.TaskFactory;
import org.apache.gobblin.runtime.task.TaskUtils;
import org.apache.gobblin.source.extractor.JobCommitPolicy;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;


/**
 * {@link Callable} that commits a single dataset. The logic in this class is thread-safe, however, it calls
 * {@link DataPublisher#publish(Collection)}. This class is thread-safe if and only if the implementation of
 * {@link DataPublisher} used is also thread-safe.
 */
@RequiredArgsConstructor
@Slf4j
final class SafeDatasetCommit implements Callable<Void> {

  private static final Object GLOBAL_LOCK = new Object();

  private static final String DATASET_STATE = "datasetState";
  private static final String FAILED_DATASET_EVENT = "failedDataset";

  private final boolean shouldCommitDataInJob;
  private final boolean isJobCancelled;
  private final DeliverySemantics deliverySemantics;
  private final String datasetUrn;
  private final JobState.DatasetState datasetState;
  private final boolean isMultithreaded;
  private final JobContext jobContext;

  private MetricContext metricContext;

  @Override
  public Void call()
      throws Exception {
    if (this.datasetState.getState() == JobState.RunningState.COMMITTED) {
      log.info(this.datasetUrn + " have been committed.");
      return null;
    }
    metricContext = Instrumented.getMetricContext(datasetState, SafeDatasetCommit.class);

    finalizeDatasetStateBeforeCommit(this.datasetState);
    Class<? extends DataPublisher> dataPublisherClass;
    try (Closer closer = Closer.create()) {
      dataPublisherClass = JobContext.getJobDataPublisherClass(this.jobContext.getJobState())
          .or((Class<? extends DataPublisher>) Class.forName(ConfigurationKeys.DEFAULT_DATA_PUBLISHER_TYPE));
      if (!canCommitDataset(datasetState)) {
        log.warn(String
            .format("Not committing dataset %s of job %s with commit policy %s and state %s", this.datasetUrn,
                this.jobContext.getJobId(), this.jobContext.getJobCommitPolicy(), this.datasetState.getState()));
        checkForUnpublishedWUHandling(this.datasetUrn, this.datasetState, dataPublisherClass, closer);
        throw new RuntimeException(String
            .format("Not committing dataset %s of job %s with commit policy %s and state %s", this.datasetUrn,
                this.jobContext.getJobId(), this.jobContext.getJobCommitPolicy(), this.datasetState.getState()));
      }
    } catch (ReflectiveOperationException roe) {
      log.error("Failed to instantiate data publisher for dataset %s of job %s.", this.datasetUrn,
          this.jobContext.getJobId(), roe);
      throw new RuntimeException(roe);
    } finally {
      maySubmitFailureEvent(datasetState);
    }

    if (this.isJobCancelled) {
      log.info("Executing commit steps although job is cancelled due to job commit policy: " + this.jobContext
          .getJobCommitPolicy());
    }

    Optional<CommitSequence.Builder> commitSequenceBuilder = Optional.absent();
    boolean canPersistStates = true;
    try (Closer closer = Closer.create()) {
      if (this.shouldCommitDataInJob) {
        log.info(String.format("Committing dataset %s of job %s with commit policy %s and state %s", this.datasetUrn,
            this.jobContext.getJobId(), this.jobContext.getJobCommitPolicy(), this.datasetState.getState()));

        ListMultimap<TaskFactoryWrapper, TaskState> taskStatesByFactory = groupByTaskFactory(this.datasetState);

        for (Map.Entry<TaskFactoryWrapper, Collection<TaskState>> entry : taskStatesByFactory.asMap().entrySet()) {
          TaskFactory taskFactory = entry.getKey().getTaskFactory();

          if (this.deliverySemantics == DeliverySemantics.EXACTLY_ONCE) {
            if (taskFactory != null) {
              throw new RuntimeException("Custom task factories do not support exactly once delivery semantics.");
            }
            generateCommitSequenceBuilder(this.datasetState, entry.getValue());
          } else {
            DataPublisher publisher = taskFactory == null ? closer
                .register(DataPublisher.getInstance(dataPublisherClass, this.jobContext.getJobState()))
                : taskFactory.createDataPublisher(this.datasetState);
            if (this.isJobCancelled) {
              if (publisher.canBeSkipped()) {
                log.warn(publisher.getClass() + " will be skipped.");
              } else {
                canPersistStates = false;
                throw new RuntimeException(
                    "Cannot persist state upon cancellation because publisher has unfinished work and cannot be skipped.");
              }
            } else if (this.isMultithreaded && !publisher.isThreadSafe()) {
              log.warn(String.format(
                  "Gobblin is set up to parallelize publishing, however the publisher %s is not thread-safe. "
                      + "Falling back to serial publishing.", publisher.getClass().getName()));
              safeCommitDataset(entry.getValue(), publisher);
            } else {
              commitDataset(entry.getValue(), publisher);
            }
          }
        }
        this.datasetState.setState(JobState.RunningState.COMMITTED);
      } else {
        if (this.datasetState.getState() == JobState.RunningState.SUCCESSFUL) {
          this.datasetState.setState(JobState.RunningState.COMMITTED);
        }
      }
    } catch (ReflectiveOperationException roe) {
      log.error(String.format("Failed to instantiate data publisher for dataset %s of job %s.", this.datasetUrn,
          this.jobContext.getJobId()), roe);
      throw new RuntimeException(roe);
    } catch (Throwable throwable) {
      log.error(String.format("Failed to commit dataset state for dataset %s of job %s", this.datasetUrn,
          this.jobContext.getJobId()), throwable);
      throw new RuntimeException(throwable);
    } finally {
      try {
        finalizeDatasetState(datasetState, datasetUrn);
        maySubmitFailureEvent(datasetState);
        maySubmitLineageEvent(datasetState);
        if (commitSequenceBuilder.isPresent()) {
          buildAndExecuteCommitSequence(commitSequenceBuilder.get(), datasetState, datasetUrn);
          datasetState.setState(JobState.RunningState.COMMITTED);
        } else if (canPersistStates) {
          persistDatasetState(datasetUrn, datasetState);
        }

      } catch (IOException | RuntimeException ioe) {
        log.error(String
            .format("Failed to persist dataset state for dataset %s of job %s", datasetUrn, this.jobContext.getJobId()),
            ioe);
        throw new RuntimeException(ioe);
      }
    }
    return null;
  }

  private void maySubmitFailureEvent(JobState.DatasetState datasetState) {
    if (datasetState.getState() == JobState.RunningState.FAILED) {
      FailureEventBuilder failureEvent = new FailureEventBuilder(FAILED_DATASET_EVENT);
      failureEvent.addMetadata(DATASET_STATE, datasetState.toString());
      failureEvent.submit(metricContext);
    }
  }

  private void maySubmitLineageEvent(JobState.DatasetState datasetState) {
    Collection<TaskState> allStates = datasetState.getTaskStates();
    Collection<TaskState> states = Lists.newArrayList();
    // Filter out failed states or states that don't have lineage info
    for (TaskState state : allStates) {
      if (state.getWorkingState() == WorkUnitState.WorkingState.COMMITTED &&
          LineageInfo.hasLineageInfo(state)) {
        states.add(state);
      }
    }
    if (states.size() == 0) {
      log.info("Will not submit lineage events as no state contains lineage info");
      return;
    }

    try {
      if (StringUtils.isEmpty(datasetUrn)) {
        // This dataset may contain different kinds of LineageEvent
        for (Map.Entry<String, Collection<TaskState>> entry : aggregateByLineageEvent(states).entrySet()) {
          submitLineageEvent(entry.getKey(), entry.getValue());
        }
      } else {
        submitLineageEvent(datasetUrn, states);
      }
    } finally {
      // Purge lineage info from all states
      for (TaskState taskState : allStates) {
        LineageInfo.purgeLineageInfo(taskState);
      }
    }
  }

  private void submitLineageEvent(String dataset, Collection<TaskState> states) {
    Collection<LineageEventBuilder> events = LineageInfo.load(states);
    // Send events
    events.forEach(event -> event.submit(metricContext));
    log.info(String.format("Submitted %d lineage events for dataset %s", events.size(), dataset));
  }

  /**
   * Synchronized version of {@link #commitDataset(Collection, DataPublisher)} used when publisher is not
   * thread safe.
   */
  private void safeCommitDataset(Collection<TaskState> taskStates, DataPublisher publisher) {
    synchronized (GLOBAL_LOCK) {
      commitDataset(taskStates, publisher);
    }
  }

  /**
   * Commit the output data of a dataset.
   */
  private void commitDataset(Collection<TaskState> taskStates, DataPublisher publisher) {

    try {
      publisher.publish(taskStates);
    } catch (Throwable t) {
      log.error("Failed to commit dataset", t);
      setTaskFailureException(taskStates, t);
    }
  }

  private ListMultimap<TaskFactoryWrapper, TaskState> groupByTaskFactory(JobState.DatasetState datasetState) {
    ListMultimap<TaskFactoryWrapper, TaskState> groupsMap = ArrayListMultimap.create();
    for (TaskState taskState : datasetState.getTaskStates()) {
      groupsMap.put(new TaskFactoryWrapper(TaskUtils.getTaskFactory(taskState).orNull()), taskState);
    }
    return groupsMap;
  }

  @Data
  private static class TaskFactoryWrapper {
    private final TaskFactory taskFactory;

    public boolean equals(Object other) {
      if (!(other instanceof TaskFactoryWrapper)) {
        return false;
      }
      if (this.taskFactory == null) {
        return ((TaskFactoryWrapper) other).taskFactory == null;
      }
      return ((TaskFactoryWrapper) other).taskFactory != null && this.taskFactory.getClass()
          .equals(((TaskFactoryWrapper) other).taskFactory.getClass());
    }

    public int hashCode() {
      final int PRIME = 59;
      int result = 1;
      final Class<?> klazz = this.taskFactory == null ? null : this.taskFactory.getClass();
      result = result * PRIME + (klazz == null ? 43 : klazz.hashCode());
      return result;
    }
  }

  private synchronized void buildAndExecuteCommitSequence(CommitSequence.Builder builder,
      JobState.DatasetState datasetState, String datasetUrn)
      throws IOException {
    CommitSequence commitSequence =
        builder.addStep(buildDatasetStateCommitStep(datasetUrn, datasetState).get()).build();
    this.jobContext.getCommitSequenceStore().get().put(commitSequence.getJobName(), datasetUrn, commitSequence);
    commitSequence.execute();
    this.jobContext.getCommitSequenceStore().get().delete(commitSequence.getJobName(), datasetUrn);
  }

  /**
   * Finalize a given {@link JobState.DatasetState} before committing the dataset.
   *
   * This method is thread-safe.
   */
  private void finalizeDatasetStateBeforeCommit(JobState.DatasetState datasetState) {
    for (TaskState taskState : datasetState.getTaskStates()) {
      if (taskState.getWorkingState() != WorkUnitState.WorkingState.SUCCESSFUL
          && this.jobContext.getJobCommitPolicy() == JobCommitPolicy.COMMIT_ON_FULL_SUCCESS) {
        // The dataset state is set to FAILED if any task failed and COMMIT_ON_FULL_SUCCESS is used
        datasetState.setState(JobState.RunningState.FAILED);
        datasetState.incrementJobFailures();
        return;
      }
    }

    datasetState.setState(JobState.RunningState.SUCCESSFUL);
    datasetState.setNoJobFailure();
  }

  /**
   * Check if it is OK to commit the output data of a dataset.
   *
   * <p>
   *   A dataset can be committed if and only if any of the following conditions is satisfied:
   *
   *   <ul>
   *     <li>The {@link JobCommitPolicy#COMMIT_ON_PARTIAL_SUCCESS} policy is used.</li>
   *     <li>The {@link JobCommitPolicy#COMMIT_SUCCESSFUL_TASKS} policy is used.</li>
   *     <li>The {@link JobCommitPolicy#COMMIT_ON_FULL_SUCCESS} policy is used and all of the tasks succeed.</li>
   *   </ul>
   * </p>
   * This method is thread-safe.
   */
  private boolean canCommitDataset(JobState.DatasetState datasetState) {
    // Only commit a dataset if 1) COMMIT_ON_PARTIAL_SUCCESS is used, or 2)
    // COMMIT_ON_FULL_SUCCESS is used and all of the tasks of the dataset have succeeded.
    return this.jobContext.getJobCommitPolicy() == JobCommitPolicy.COMMIT_ON_PARTIAL_SUCCESS
        || this.jobContext.getJobCommitPolicy() == JobCommitPolicy.COMMIT_SUCCESSFUL_TASKS || (
        this.jobContext.getJobCommitPolicy() == JobCommitPolicy.COMMIT_ON_FULL_SUCCESS
            && datasetState.getState() == JobState.RunningState.SUCCESSFUL);
  }

  @SuppressWarnings("unchecked")
  private Optional<CommitSequence.Builder> generateCommitSequenceBuilder(JobState.DatasetState datasetState,
      Collection<TaskState> taskStates) {
    try (Closer closer = Closer.create()) {
      Class<? extends CommitSequencePublisher> dataPublisherClass = (Class<? extends CommitSequencePublisher>) Class
          .forName(datasetState
              .getProp(ConfigurationKeys.DATA_PUBLISHER_TYPE, ConfigurationKeys.DEFAULT_DATA_PUBLISHER_TYPE));
      CommitSequencePublisher publisher = (CommitSequencePublisher) closer
          .register(DataPublisher.getInstance(dataPublisherClass, this.jobContext.getJobState()));
      publisher.publish(taskStates);
      return publisher.getCommitSequenceBuilder();
    } catch (Throwable t) {
      log.error("Failed to generate commit sequence", t);
      setTaskFailureException(datasetState.getTaskStates(), t);
      throw Throwables.propagate(t);
    }
  }

  void checkForUnpublishedWUHandling(String datasetUrn, JobState.DatasetState datasetState,
      Class<? extends DataPublisher> dataPublisherClass, Closer closer)
      throws ReflectiveOperationException, IOException {
    if (UnpublishedHandling.class.isAssignableFrom(dataPublisherClass)) {
      // pass in jobstate to retrieve properties
      DataPublisher publisher =
          closer.register(DataPublisher.getInstance(dataPublisherClass, this.jobContext.getJobState()));
      log.info(String.format("Calling publisher to handle unpublished work units for dataset %s of job %s.", datasetUrn,
          this.jobContext.getJobId()));
      ((UnpublishedHandling) publisher).handleUnpublishedWorkUnits(datasetState.getTaskStatesAsWorkUnitStates());
    }
  }

  private void finalizeDatasetState(JobState.DatasetState datasetState, String datasetUrn) {
    for (TaskState taskState : datasetState.getTaskStates()) {
      // Backoff the actual high watermark to the low watermark for each task that has not been committed
      if (taskState.getWorkingState() != WorkUnitState.WorkingState.COMMITTED) {
        taskState.backoffActualHighWatermark();
        if (this.jobContext.getJobCommitPolicy() == JobCommitPolicy.COMMIT_ON_FULL_SUCCESS) {
          // Determine the final dataset state based on the task states (post commit) and the job commit policy.
          // 1. If COMMIT_ON_FULL_SUCCESS is used, the processing of the dataset is considered failed if any
          //    task for the dataset failed to be committed.
          // 2. Otherwise, the processing of the dataset is considered successful even if some tasks for the
          //    dataset failed to be committed.
          datasetState.setState(JobState.RunningState.FAILED);
        }
      }
    }

    datasetState.setId(datasetUrn);
  }

  /**
   * Persist dataset state of a given dataset identified by the dataset URN.
   */
  private void persistDatasetState(String datasetUrn, JobState.DatasetState datasetState)
      throws IOException {
    log.info("Persisting dataset state for dataset " + datasetUrn);
    this.jobContext.getDatasetStateStore().persistDatasetState(datasetUrn, datasetState);
  }

  /**
   * Sets the {@link ConfigurationKeys#TASK_FAILURE_EXCEPTION_KEY} for each given {@link TaskState} to the given
   * {@link Throwable}.
   *
   * Make this method public as this exception catching routine can be reusable in other occasions as well.
   */
  public static void setTaskFailureException(Collection<? extends WorkUnitState> taskStates, Throwable t) {
    for (WorkUnitState taskState : taskStates) {
      ((TaskState) taskState).setTaskFailureException(t);
    }
  }

  private static Optional<CommitStep> buildDatasetStateCommitStep(String datasetUrn,
      JobState.DatasetState datasetState) {

    log.info("Creating " + DatasetStateCommitStep.class.getSimpleName() + " for dataset " + datasetUrn);
    return Optional.of(new DatasetStateCommitStep.Builder<>().withProps(datasetState).withDatasetUrn(datasetUrn)
        .withDatasetState(datasetState).build());
  }

  private static Map<String, Collection<TaskState>> aggregateByLineageEvent(Collection<TaskState> states) {
    Map<String, Collection<TaskState>> statesByEvents = Maps.newHashMap();
    for (TaskState state : states) {
      String eventName = LineageInfo.getFullEventName(state);
      Collection<TaskState> statesForEvent = statesByEvents.computeIfAbsent(eventName, k -> Lists.newArrayList());
      statesForEvent.add(state);
    }

    return statesByEvents;
  }
}
