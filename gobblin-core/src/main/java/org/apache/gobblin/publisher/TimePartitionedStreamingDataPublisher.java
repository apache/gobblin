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
package org.apache.gobblin.publisher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.Path;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.event.lineage.LineageInfo;
import org.apache.gobblin.util.ForkOperatorUtils;
import org.apache.gobblin.util.ParallelRunner;
import org.apache.gobblin.util.WriterUtils;


@Slf4j
public class TimePartitionedStreamingDataPublisher extends TimePartitionedDataPublisher {
  private final MetricContext metricContext;
  public TimePartitionedStreamingDataPublisher(State state) throws IOException {
    super(state);
    this.metricContext = Instrumented.getMetricContext(state, TimePartitionedStreamingDataPublisher.class);
  }

  /**
   * This method publishes task output data for the given {@link WorkUnitState}, but if there are output data of
   * other tasks in the same folder, it may also publish those data.
   */
  protected void publishMultiTaskData(WorkUnitState state, int branchId, Set<Path> writerOutputPathsMoved)
      throws IOException {
    state.setProp(ConfigurationKeys.DATA_PUBLISHER_DATASET_DIR, this.getPublisherOutputDir(state, branchId).toString());
    super.publishMultiTaskData(state, branchId, writerOutputPathsMoved);
  }

  @Override
  protected void publishData(WorkUnitState state, int branchId, boolean publishSingleTaskData,
      Set<Path> writerOutputPathsMoved) throws IOException {
    // The directory where the workUnitState wrote its output data.
    Path writerOutputDir = WriterUtils.getWriterOutputDir(state, this.numBranches, branchId);

    if (!this.writerFileSystemByBranches.get(branchId).exists(writerOutputDir)) {
      log.warn(String.format("Branch %d of WorkUnit %s produced no data", branchId, state.getId()));
      return;
    }
    // The directory where the final output directory for this job will be placed.
    // It is a combination of DATA_PUBLISHER_FINAL_DIR and WRITER_FILE_PATH.
    Path publisherOutputDir = getPublisherOutputDir(state, branchId);

    if (!this.publisherFileSystemByBranches.get(branchId).exists(publisherOutputDir)) {
      // Create the directory of the final output directory if it does not exist before we do the actual publish
      // This is used to force the publisher save recordPublisherOutputDirs as the granularity to be parent of new file paths
      // which will be used to do hive registration
      WriterUtils.mkdirsWithRecursivePermissionWithRetry(this.publisherFileSystemByBranches.get(branchId),
          publisherOutputDir, this.permissions.get(branchId), retryerConfig);
    }
    super.publishData(state, branchId, publishSingleTaskData, writerOutputPathsMoved);
  }

  @Override
  public void publishData(Collection<? extends WorkUnitState> states) throws IOException {
    publishDataImpl(states);
    //Clean up state to remove filenames which have been committed from the state object
    wusCleanUp(states);
  }

  public void publishDataImpl(Collection<? extends WorkUnitState> states) throws IOException {

    // We need a Set to collect unique writer output paths as multiple tasks may belong to the same extract. Tasks that
    // belong to the same Extract will by default have the same output directory
    Set<Path> writerOutputPathsMoved = Sets.newHashSet();

    for (WorkUnitState workUnitState : states) {
      for (int branchId = 0; branchId < this.numBranches; branchId++) {
        publishMultiTaskData(workUnitState, branchId, writerOutputPathsMoved);
      }
    }

    //Wait for any submitted ParallelRunner threads to finish
    for (ParallelRunner runner : this.parallelRunners.values()) {
      runner.waitForTasks();
    }

    for (WorkUnitState workUnitState : states) {
      // Upon successfully committing the data to the final output directory, set states
      // of successful tasks to COMMITTED. leaving states of unsuccessful ones unchanged.
      // This makes sense to the COMMIT_ON_PARTIAL_SUCCESS policy.
      workUnitState.setWorkingState(WorkUnitState.WorkingState.COMMITTED);
    }

    ArrayList<WorkUnitState> statesWithLineage = Lists.newArrayList();

    for (WorkUnitState state: states) {
      if (LineageInfo.hasLineageInfo(state)) {
        statesWithLineage.add(state);
      }
    }
    long startTime = System.currentTimeMillis();
    submitLineageEvents(statesWithLineage);
    log.info("Emitting lineage events took {} millis", System.currentTimeMillis() - startTime);
  }

  private void submitLineageEvents(Collection<? extends WorkUnitState> states) {
    for (Map.Entry<String, Collection<WorkUnitState>> entry : LineageInfo.aggregateByLineageEvent(states).entrySet()) {
      LineageInfo.submitLineageEvent(entry.getKey(), entry.getValue(), metricContext);
    }
  }

  /**
   * A helper method to clean up {@link WorkUnitState}.
   * @param states
   */
  protected void wusCleanUp(Collection<? extends WorkUnitState> states) {
    // use the first work unit state to get common properties
    WorkUnitState wuState = states.stream().findFirst().get();
    int numBranches = wuState.getPropAsInt("fork.branches", 1);

    // clean up state kept for data publishing
    for (WorkUnitState state : states) {
      for (int branchId = 0; branchId < numBranches; branchId++) {
        String outputFilePropName =
            ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_FINAL_OUTPUT_FILE_PATHS, numBranches,
                branchId);

        if (state.contains(outputFilePropName)) {
          state.removeProp(outputFilePropName);
        }

        LineageInfo.removeDestinationProp(state, branchId);
      }
    }
  }

  @VisibleForTesting
  Set<Path> getPublishOutputDirs() {
    return this.publisherOutputDirs;
  }
}