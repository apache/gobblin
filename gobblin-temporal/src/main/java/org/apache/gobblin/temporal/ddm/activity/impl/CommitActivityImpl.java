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
import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.typesafe.config.ConfigFactory;
import io.temporal.failure.ApplicationFailure;
import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.broker.SharedResourcesBrokerFactory;
import org.apache.gobblin.broker.gobblin_scopes.GobblinScopeTypes;
import org.apache.gobblin.broker.iface.SharedResourcesBroker;
import org.apache.gobblin.commit.DeliverySemantics;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.metastore.FsStateStore;
import org.apache.gobblin.metastore.StateStore;
import org.apache.gobblin.runtime.JobContext;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.runtime.SafeDatasetCommit;
import org.apache.gobblin.runtime.TaskState;
import org.apache.gobblin.runtime.TaskStateCollectorService;
import org.apache.gobblin.source.extractor.JobCommitPolicy;
import org.apache.gobblin.temporal.ddm.activity.CommitActivity;
import org.apache.gobblin.temporal.ddm.util.JobStateUtils;
import org.apache.gobblin.temporal.ddm.work.WUProcessingSpec;
import org.apache.gobblin.temporal.ddm.work.WorkUnitClaimCheck;
import org.apache.gobblin.temporal.ddm.work.assistance.Help;
import org.apache.gobblin.util.Either;
import org.apache.gobblin.util.ExecutorsUtils;
import org.apache.gobblin.util.HadoopUtils;
import org.apache.gobblin.util.SerializationUtils;
import org.apache.gobblin.util.executors.IteratorExecutor;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


@Slf4j
public class CommitActivityImpl implements CommitActivity {

  @Override
  public int commit(WUProcessingSpec workSpec) {
    int numDeserializationThreads = 1;
    try {
      FileSystem fs = Help.loadFileSystem(workSpec);
      JobState jobState = Help.loadJobState(workSpec, fs);
      SharedResourcesBroker<GobblinScopeTypes> instanceBroker = createDefaultInstanceBroker(jobState.getProperties());
      JobContext globalGobblinContext = new JobContext(jobState.getProperties(), log, instanceBroker, null);
      // TODO: Task state dir is a stub with the assumption it is always colocated with the workunits dir (as in the case of MR which generates workunits)
      Path jobIdParent = new Path(workSpec.getWorkUnitsDir()).getParent();
      Path jobOutputPath = new Path(new Path(jobIdParent, "output"), jobIdParent.getName());
      log.info("Output path at: " + jobOutputPath + " with fs at " + fs.getUri());
      StateStore<TaskState> taskStateStore = Help.openTaskStateStore(workSpec, fs);
      Collection<TaskState> taskStateQueue =
          ImmutableList.copyOf(
              TaskStateCollectorService.deserializeTaskStatesFromFolder(taskStateStore, jobOutputPath, numDeserializationThreads));
      commitTaskStates(jobState, taskStateQueue, globalGobblinContext);

    } catch (Exception e) {
      //TODO: IMPROVE GRANULARITY OF RETRIES
      throw ApplicationFailure.newNonRetryableFailureWithCause(
          "Failed to commit dataset state for some dataset(s) of job <jobStub>",
          IOException.class.toString(),
          new IOException(e),
          null
      );
    }

    return 0;
  }

  protected FileSystem loadFileSystemForUri(URI fsUri, State stateConfig) throws IOException {
    return HadoopUtils.getFileSystem(fsUri, stateConfig);
  }

  void commitTaskStates(State jobState, Collection<TaskState> taskStates, JobContext jobContext) throws IOException {
    Map<String, JobState.DatasetState> datasetStatesByUrns = createDatasetStatesByUrns(taskStates);
    final boolean shouldCommitDataInJob = shouldCommitDataInJob(jobState);
    final DeliverySemantics deliverySemantics = DeliverySemantics.AT_LEAST_ONCE;
    final int numCommitThreads = 1;

    if (!shouldCommitDataInJob) {
      log.info("Job will not commit data since data are committed by tasks.");
    }

    try {
      if (!datasetStatesByUrns.isEmpty()) {
        log.info("Persisting dataset urns.");
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
          ExecutorsUtils.newThreadFactory(Optional.of(log), Optional.of("Commit-thread-%d")))
          .executeAndGetResults();

      IteratorExecutor.logFailures(result, null, 10);

      if (!IteratorExecutor.verifyAllSuccessful(result)) {
        // TODO: propagate cause of failure
        throw new IOException("Failed to commit dataset state for some dataset(s) of job <jobStub>");
      }
    } catch (InterruptedException exc) {
      throw new IOException(exc);
    }
  }

  public Map<String, JobState.DatasetState> createDatasetStatesByUrns(Collection<TaskState> taskStates) {
    Map<String, JobState.DatasetState> datasetStatesByUrns = Maps.newHashMap();

    //TODO: handle skipped tasks?
    for (TaskState taskState : taskStates) {
      String datasetUrn = createDatasetUrn(datasetStatesByUrns, taskState);
      datasetStatesByUrns.get(datasetUrn).incrementTaskCount();
      datasetStatesByUrns.get(datasetUrn).addTaskState(taskState);
    }

    return datasetStatesByUrns;
  }

  private String createDatasetUrn(Map<String, JobState.DatasetState> datasetStatesByUrns, TaskState taskState) {
    String datasetUrn = taskState.getProp(ConfigurationKeys.DATASET_URN_KEY, ConfigurationKeys.DEFAULT_DATASET_URN);
    if (!datasetStatesByUrns.containsKey(datasetUrn)) {
      JobState.DatasetState datasetState = new JobState.DatasetState();
      datasetState.setDatasetUrn(datasetUrn);
      datasetStatesByUrns.put(datasetUrn, datasetState);
    }
    return datasetUrn;
  }

  private static boolean shouldCommitDataInJob(State state) {
    boolean jobCommitPolicyIsFull =
        JobCommitPolicy.getCommitPolicy(state.getProperties()) == JobCommitPolicy.COMMIT_ON_FULL_SUCCESS;
    boolean publishDataAtJobLevel = state.getPropAsBoolean(ConfigurationKeys.PUBLISH_DATA_AT_JOB_LEVEL,
        ConfigurationKeys.DEFAULT_PUBLISH_DATA_AT_JOB_LEVEL);
    boolean jobDataPublisherSpecified =
        !Strings.isNullOrEmpty(state.getProp(ConfigurationKeys.JOB_DATA_PUBLISHER_TYPE));
    return jobCommitPolicyIsFull || publishDataAtJobLevel || jobDataPublisherSpecified;
  }

  private static SharedResourcesBroker<GobblinScopeTypes> createDefaultInstanceBroker(Properties jobProps) {
    log.warn("Creating a job specific {}. Objects will only be shared at the job level.",
        SharedResourcesBroker.class.getSimpleName());
    return SharedResourcesBrokerFactory.createDefaultTopLevelBroker(ConfigFactory.parseProperties(jobProps),
        GobblinScopeTypes.GLOBAL.defaultScopeInstance());
  }

}
