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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.broker.gobblin_scopes.GobblinScopeTypes;
import org.apache.gobblin.broker.iface.SharedResourcesBroker;
import org.apache.gobblin.commit.DeliverySemantics;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.metastore.StateStore;
import org.apache.gobblin.runtime.JobContext;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.runtime.SafeDatasetCommit;
import org.apache.gobblin.runtime.TaskState;
import org.apache.gobblin.runtime.TaskStateCollectorService;
import org.apache.gobblin.temporal.ddm.activity.CommitActivity;
import org.apache.gobblin.temporal.ddm.util.JobStateUtils;
import org.apache.gobblin.temporal.ddm.work.WUProcessingSpec;
import org.apache.gobblin.temporal.ddm.work.assistance.Help;
import org.apache.gobblin.util.Either;
import org.apache.gobblin.util.ExecutorsUtils;
import org.apache.gobblin.util.executors.IteratorExecutor;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


@Slf4j
public class CommitActivityImpl implements CommitActivity {

  static int DEFAULT_NUM_DESERIALIZATION_THREADS = 10;
  static int DEFAULT_NUM_COMMIT_THREADS = 1;
  @Override
  public int commit(WUProcessingSpec workSpec) {
    // TODO: Make this configurable
    int numDeserializationThreads = DEFAULT_NUM_DESERIALIZATION_THREADS;
    try {
      FileSystem fs = Help.loadFileSystem(workSpec);
      JobState jobState = Help.loadJobState(workSpec, fs);
      SharedResourcesBroker<GobblinScopeTypes> instanceBroker = JobStateUtils.getSharedResourcesBroker(jobState);
      JobContext globalGobblinContext = new JobContext(jobState.getProperties(), log, instanceBroker, null);
      // TODO: Task state dir is a stub with the assumption it is always colocated with the workunits dir (as in the case of MR which generates workunits)
      Path jobIdParent = new Path(workSpec.getWorkUnitsDir()).getParent();
      Path jobOutputPath = new Path(new Path(jobIdParent, "output"), jobIdParent.getName());
      log.info("Output path at: " + jobOutputPath + " with fs at " + fs.getUri());
      StateStore<TaskState> taskStateStore = Help.openTaskStateStore(workSpec, fs);
      Optional<Queue<TaskState>> taskStateQueue =
              TaskStateCollectorService.deserializeTaskStatesFromFolder(taskStateStore, jobOutputPath.getName(), numDeserializationThreads);
      if (!taskStateQueue.isPresent()) {
        log.error("No task states found at " + jobOutputPath);
        return 0;
      }
      commitTaskStates(jobState, ImmutableList.copyOf(taskStateQueue.get()), globalGobblinContext);
      return taskStateQueue.get().size();
    } catch (Exception e) {
      //TODO: IMPROVE GRANULARITY OF RETRIES
      throw ApplicationFailure.newNonRetryableFailureWithCause(
          "Failed to commit dataset state for some dataset(s) of job <jobStub>",
          IOException.class.toString(),
          new IOException(e),
          null
      );
    }
  }

  /**
   * Commit task states to the dataset state store.
   * @param jobState
   * @param taskStates
   * @param jobContext
   * @throws IOException
   */
  private void commitTaskStates(State jobState, Collection<TaskState> taskStates, JobContext jobContext) throws IOException {
    Map<String, JobState.DatasetState> datasetStatesByUrns = createDatasetStatesByUrns(taskStates);
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

      if (!IteratorExecutor.verifyAllSuccessful(result)) {
        // TODO: propagate cause of failure and determine whether or not this is retryable to throw a non-retryable failure exception
        String jobName = jobState.getProperties().getProperty(ConfigurationKeys.JOB_NAME_KEY, "<job_name_stub>");
        throw new IOException("Failed to commit dataset state for some dataset(s) of job " + jobName);
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
