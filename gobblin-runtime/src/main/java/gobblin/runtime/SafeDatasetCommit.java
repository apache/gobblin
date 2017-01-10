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

package gobblin.runtime;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.Callable;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.io.Closer;

import gobblin.commit.CommitSequence;
import gobblin.commit.CommitStep;
import gobblin.commit.DeliverySemantics;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.publisher.CommitSequencePublisher;
import gobblin.publisher.DataPublisher;
import gobblin.publisher.UnpublishedHandling;
import gobblin.runtime.commit.DatasetStateCommitStep;
import gobblin.source.extractor.JobCommitPolicy;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;


/**
 * {@link Callable} that commits a single dataset. The logic in this class is thread-safe, however, it calls
 * {@link DataPublisher#publish(Collection)}. This class is thread-safe if and only if the implementation of
 * {@link DataPublisher} used is also thread-safe.
 */
@AllArgsConstructor
@Slf4j
final class SafeDatasetCommit implements Callable<Void> {

  private final boolean shouldCommitDataInJob;
  private final DeliverySemantics deliverySemantics;
  private final String datasetUrn;
  private final JobState.DatasetState datasetState;
  private final boolean isMultithreaded;
  private final JobContext jobContext;

  @Override
  public Void call()
      throws Exception {

    finalizeDatasetStateBeforeCommit(this.datasetState);

    Class<? extends DataPublisher> dataPublisherClass;
    try (Closer closer = Closer.create()) {
      dataPublisherClass = JobContext.getJobDataPublisherClass(this.jobContext.getJobState())
          .or((Class<? extends DataPublisher>) Class.forName(ConfigurationKeys.DEFAULT_DATA_PUBLISHER_TYPE));
      if (!canCommitDataset(datasetState)) {
        log.warn(String.format("Not committing dataset %s of job %s with commit policy %s and state %s",
            this.datasetUrn, this.jobContext.getJobId(), this.jobContext.getJobCommitPolicy(), this.datasetState.getState()));
        checkForUnpublishedWUHandling(this.datasetUrn, this.datasetState, dataPublisherClass, closer);
        throw new RuntimeException(String.format("Not committing dataset %s of job %s with commit policy %s and state %s",
            this.datasetUrn, this.jobContext.getJobId(), this.jobContext.getJobCommitPolicy(), this.datasetState.getState()));
      }
    } catch (ReflectiveOperationException roe) {
      log.error("Failed to instantiate data publisher for dataset %s of job %s.", this.datasetUrn,
          this.jobContext.getJobId(), roe);
      throw new RuntimeException(roe);
    }

    Optional<CommitSequence.Builder> commitSequenceBuilder = Optional.absent();
    try (Closer closer = Closer.create()) {
      if (this.shouldCommitDataInJob) {
        log.info(String.format("Committing dataset %s of job %s with commit policy %s and state %s", this.datasetUrn,
            this.jobContext.getJobId(), this.jobContext.getJobCommitPolicy(), this.datasetState.getState()));
        if (this.deliverySemantics == DeliverySemantics.EXACTLY_ONCE) {
          generateCommitSequenceBuilder(this.datasetState);
        } else {
          DataPublisher publisher = closer.register(DataPublisher.getInstance(dataPublisherClass, this.jobContext.getJobState()));
          if (this.isMultithreaded && !publisher.isThreadSafe()) {
            log.warn(String.format("Gobblin is set up to parallelize publishing, however the publisher %s is not thread-safe. "
                + "Falling back to serial publishing.", publisher.getClass().getName()));
            safeCommitDataset(this.datasetState, publisher);
          } else {
            commitDataset(this.datasetState, publisher);
          }
        }
      } else {
        if (this.datasetState.getState() == JobState.RunningState.SUCCESSFUL) {
          this.datasetState.setState(JobState.RunningState.COMMITTED);
        }
      }
    } catch (ReflectiveOperationException roe) {
      log.error(String.format("Failed to instantiate data publisher for dataset %s of job %s.", this.datasetUrn,
              this.jobContext.getJobId()), roe);
      throw new RuntimeException(roe);
    } catch (IOException ioe) {
      log.error(String.format("Failed to commit dataset state for dataset %s of job %s", this.datasetUrn,
          this.jobContext.getJobId()), ioe);
      throw new RuntimeException(ioe);
    } finally {
      try {
        finalizeDatasetState(datasetState, datasetUrn);

        if (commitSequenceBuilder.isPresent()) {
          buildAndExecuteCommitSequence(commitSequenceBuilder.get(), datasetState, datasetUrn);
          datasetState.setState(JobState.RunningState.COMMITTED);
        } else {
          persistDatasetState(datasetUrn, datasetState);
        }
      } catch (IOException | RuntimeException ioe) {
        log.error(
            String.format("Failed to persist dataset state for dataset %s of job %s", datasetUrn, this.jobContext.getJobId()), ioe);
        throw new RuntimeException(ioe);
      }
    }
    return null;
  }

  /**
   * Synchronized version of {@link #commitDataset(JobState.DatasetState, DataPublisher)} used when publisher is not
   * thread safe.
   */
  private synchronized void safeCommitDataset(JobState.DatasetState datasetState, DataPublisher publisher) {
    commitDataset(datasetState, publisher);
  }

  /**
   * Commit the output data of a dataset.
   */
  private void commitDataset(JobState.DatasetState datasetState, DataPublisher publisher) {

    try {
      publisher.publish(datasetState.getTaskStates());
    } catch (Throwable t) {
      log.error("Failed to commit dataset", t);
      setTaskFailureException(datasetState.getTaskStates(), t);
    }

    // Set the dataset state to COMMITTED upon successful commit
    datasetState.setState(JobState.RunningState.COMMITTED);
  }

  private synchronized void buildAndExecuteCommitSequence(CommitSequence.Builder builder, JobState.DatasetState datasetState,
      String datasetUrn) throws IOException {
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
        || this.jobContext.getJobCommitPolicy() == JobCommitPolicy.COMMIT_SUCCESSFUL_TASKS
        || (this.jobContext.getJobCommitPolicy() == JobCommitPolicy.COMMIT_ON_FULL_SUCCESS
        && datasetState.getState() == JobState.RunningState.SUCCESSFUL);
  }

  @SuppressWarnings("unchecked")
  private Optional<CommitSequence.Builder> generateCommitSequenceBuilder(JobState.DatasetState datasetState) {
    try (Closer closer = Closer.create()) {
      Class<? extends CommitSequencePublisher> dataPublisherClass =
          (Class<? extends CommitSequencePublisher>) Class.forName(datasetState
              .getProp(ConfigurationKeys.DATA_PUBLISHER_TYPE, ConfigurationKeys.DEFAULT_DATA_PUBLISHER_TYPE));
      CommitSequencePublisher publisher =
          (CommitSequencePublisher) closer.register(DataPublisher.getInstance(dataPublisherClass, this.jobContext.getJobState()));
      publisher.publish(datasetState.getTaskStates());
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
      DataPublisher publisher = closer.register(DataPublisher.getInstance(dataPublisherClass, this.jobContext.getJobState()));
      log.info(String.format("Calling publisher to handle unpublished work units for dataset %s of job %s.",
          datasetUrn, this.jobContext.getJobId()));
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
  private void persistDatasetState(String datasetUrn, JobState.DatasetState datasetState) throws IOException {
    log.info("Persisting dataset state for dataset " + datasetUrn);
    this.jobContext.getDatasetStateStore().persistDatasetState(datasetUrn, datasetState);
  }

  /**
   * Sets the {@link ConfigurationKeys#TASK_FAILURE_EXCEPTION_KEY} for each given {@link TaskState} to the given
   * {@link Throwable}.
   */
  private static void setTaskFailureException(Collection<TaskState> taskStates, Throwable t) {
    for (TaskState taskState : taskStates) {
      taskState.setTaskFailureException(t);
    }
  }

  private static Optional<CommitStep> buildDatasetStateCommitStep(String datasetUrn,
      JobState.DatasetState datasetState) {

    log.info("Creating " + DatasetStateCommitStep.class.getSimpleName() + " for dataset " + datasetUrn);
    return Optional.of(new DatasetStateCommitStep.Builder<>().withProps(datasetState).withDatasetUrn(datasetUrn)
        .withDatasetState(datasetState).build());
  }
}
