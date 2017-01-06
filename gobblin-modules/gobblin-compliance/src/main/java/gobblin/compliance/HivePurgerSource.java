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
package gobblin.compliance;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.ql.metadata.Partition;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;

import com.google.common.base.Throwables;
import lombok.extern.slf4j.Slf4j;

import gobblin.configuration.SourceState;
import gobblin.configuration.WorkUnitState;
import gobblin.data.management.copy.hive.HiveDataset;
import gobblin.data.management.copy.hive.HiveDatasetFinder;
import gobblin.dataset.IterableDatasetFinder;
import gobblin.source.Source;
import gobblin.source.extractor.Extractor;
import gobblin.source.workunit.WorkUnit;
import gobblin.util.HadoopUtils;


/**
 * This class creates {@link WorkUnit}s and assigns exactly one partition to each of them.
 * It iterates over all Hive Tables specified via whitelist and blacklist, list all partitions, and create
 * workunits accordingly based on jobWatermark.
 *
 * It revive {@link WorkUnit}s if their execution attempts are not exhausted.
 *
 * @author adsharma
 */
@Slf4j
public class HivePurgerSource implements Source {
  private static final Splitter AT_SPLITTER = Splitter.on("@").omitEmptyStrings().trimResults();

  protected IterableDatasetFinder<HiveDataset> datasetFinder;
  protected Map<String, WorkUnit> workUnitMap = new HashMap<>();
  protected int maxWorkUnitExecutionAttempts;
  protected int maxWorkUnits;
  protected int workUnitsCreatedCount = 0;
  protected String lowWatermark;
  private String timeStamp;

  // These datasets are lexicographically sorted by their name
  protected List<HiveDataset> datasets = new ArrayList<>();

  @VisibleForTesting
  protected void initialize(SourceState state)
      throws IOException {
    setTimeStamp();
    this.setLowWatermark(state);
    this.maxWorkUnits = state
        .getPropAsInt(HivePurgerConfigurationKeys.MAX_WORKUNITS_KEY, HivePurgerConfigurationKeys.DEFAULT_MAX_WORKUNITS);
    this.maxWorkUnitExecutionAttempts = state
        .getPropAsInt(HivePurgerConfigurationKeys.MAX_WORKUNIT_EXECUTION_ATTEMPTS_KEY,
            HivePurgerConfigurationKeys.DEFAULT_MAX_WORKUNIT_EXECUTION_ATTEMPTS);
    // TODO: Event submitter and metrics will be added later
    this.datasetFinder =
        new HiveDatasetFinder(FileSystem.newInstance(HadoopUtils.newConfiguration()), state.getProperties());
    populateDatasets();
  }

  @Override
  public List<WorkUnit> getWorkunits(SourceState state) {
    try {
      initialize(state);
      createWorkUnits(state);
    } catch (IOException e) {
      Throwables.propagate(e);
    }
    return new ArrayList<>(this.workUnitMap.values());
  }

  private WorkUnit createNewWorkUnit(String partitionName, int executionAttempts) {
    WorkUnit workUnit = WorkUnit.createEmpty();
    workUnit.setProp(HivePurgerConfigurationKeys.PARTITION_NAME, partitionName);
    workUnit.setProp(HivePurgerConfigurationKeys.EXECUTION_ATTEMPTS, executionAttempts);
    workUnit.setProp(HivePurgerConfigurationKeys.HIVE_PURGER_JOB_TIMESTAMP, this.timeStamp);
    return workUnit;
  }

  private WorkUnit createNewWorkUnit(String partitionName) {
    return createNewWorkUnit(partitionName, HivePurgerConfigurationKeys.DEFAULT_EXECUTION_ATTEMPTS);
  }

  /**
   * This method creates the list of all work units needed for the current execution.
   * Fresh work units are created for each partition starting from watermark and failed work units from the
   * previous run will be added to the list.
   */
  protected void createWorkUnits(SourceState state)
      throws IOException {
    createWorkunitsFromPreviousState(state);
    if (this.datasets.isEmpty()) {
      return;
    }
    for (HiveDataset hiveDataset : this.datasets) {
      if (!shouldPurgeDataset(hiveDataset)) {
        continue;
      }
      for (Partition partition : hiveDataset.getPartitionsFromDataset()) {
        if (workUnitsExceeded()) {
          setJobWatermark(state, partition.getCompleteName());
          return;
        }
        if (!shouldPurgePartition(partition, state)) {
          continue;
        }
        WorkUnit workUnit = createNewWorkUnit(partition.getCompleteName());
        log.info(
            "Created new work unit with partition " + workUnit.getProp(HivePurgerConfigurationKeys.PARTITION_NAME));
        this.workUnitMap.put(workUnit.getProp(HivePurgerConfigurationKeys.PARTITION_NAME), workUnit);
        this.workUnitsCreatedCount++;
      }
    }
    if (!state.contains(HivePurgerConfigurationKeys.HIVE_PURGER_WATERMARK)) {
      this.setJobWatermark(state, HivePurgerConfigurationKeys.NO_PREVIOUS_WATERMARK);
    }
  }

  private boolean workUnitsExceeded() {
    return !(this.workUnitsCreatedCount < this.maxWorkUnits);
  }

  /**
   * Find all datasets on the basis on whitelist and blacklist, and then add them in a list in lexicographical order.
   */
  protected void populateDatasets()
      throws IOException {
    Iterator<HiveDataset> iterator = this.datasetFinder.getDatasetsIterator();
    while (iterator.hasNext()) {
      this.datasets.add(iterator.next());
    }
    sortHiveDatasets(datasets);
  }

  /**
   * Sort all HiveDatasets on the basis of complete name ie dbName.tableName
   */
  private List<HiveDataset> sortHiveDatasets(List<HiveDataset> hiveDatasets) {
    Collections.sort(hiveDatasets, new Comparator<HiveDataset>() {
      @Override
      public int compare(HiveDataset o1, HiveDataset o2) {
        return o1.getTable().getCompleteName().compareTo(o2.getTable().getCompleteName());
      }
    });
    return hiveDatasets;
  }

  /**
   * Fetches the value of a watermark given its key from the previous run.
   */
  protected String getWatermarkFromPreviousWorkUnits(SourceState state, String watermark) {
    if (state.getPreviousWorkUnitStates().isEmpty()) {
      return HivePurgerConfigurationKeys.NO_PREVIOUS_WATERMARK;
    }
    return state.getPreviousWorkUnitStates().get(0)
        .getProp(watermark, HivePurgerConfigurationKeys.NO_PREVIOUS_WATERMARK);
  }

  /**
   * Job watermark corresponds to the complete partition name ie dbName.tableName.partitionName
   * This method returns the complete table name ie dbName.tableName
   */
  private String getCompleteTableNameFromJobWatermark(String jobWatermark) {
    if (jobWatermark.equalsIgnoreCase(HivePurgerConfigurationKeys.NO_PREVIOUS_WATERMARK)) {
      return HivePurgerConfigurationKeys.NO_PREVIOUS_WATERMARK;
    }
    List<String> list = AT_SPLITTER.splitToList(jobWatermark);
    Preconditions.checkArgument(list.size() == 3, "Job watermark is not in correct format");
    return list.get(0) + "@" + list.get(1);
  }

  /**
   * Decides if the given dataset should be purged in the current run.
   * If a Hive table is not an EXTERNAL table, it won't be purged.
   * The dataset must lie after the job watermark in the dictionary order.
   */
  protected boolean shouldPurgeDataset(HiveDataset hiveDataset) {
    if (!hiveDataset.getTable().getMetadata().containsKey(HivePurgerConfigurationKeys.EXTERNAL)) {
      return false;
    }

    String tableName = hiveDataset.getTable().getCompleteName();
    if (getLowWatermark().equals(HivePurgerConfigurationKeys.NO_PREVIOUS_WATERMARK)) {
      return true;
    }
    return tableName.compareTo(getCompleteTableNameFromJobWatermark(getLowWatermark())) >= 0;
  }

  /**
   * Decides if the given partition should be purged in the current run.
   * The partition must lie after the job watermark in the dictionary order.
   */
  protected boolean shouldPurgePartition(Partition partition, SourceState state) {
    if (getLowWatermark().equals(HivePurgerConfigurationKeys.NO_PREVIOUS_WATERMARK)) {
      return true;
    }
    return partition.getCompleteName().compareTo(getLowWatermark()) >= 0;
  }

  /**
   * Add failed work units in a workUnitMap with partition name as Key.
   * New work units are created using required configuration from the old work unit.
   */
  protected void createWorkunitsFromPreviousState(SourceState state) {
    if (getLowWatermark().equalsIgnoreCase(HivePurgerConfigurationKeys.NO_PREVIOUS_WATERMARK)) {
      return;
    }
    if (Iterables.isEmpty(state.getPreviousWorkUnitStates())) {
      return;
    }
    for (WorkUnitState workUnitState : state.getPreviousWorkUnitStates()) {
      if (workUnitState.getWorkingState() == WorkUnitState.WorkingState.COMMITTED) {
        continue;
      }
      WorkUnit workUnit = workUnitState.getWorkunit();
      Preconditions.checkArgument(workUnit.contains(HivePurgerConfigurationKeys.PARTITION_NAME),
          "Older WorkUnit doesn't contain property partition name.");
      int executionAttempts = workUnit.getPropAsInt(HivePurgerConfigurationKeys.EXECUTION_ATTEMPTS,
          HivePurgerConfigurationKeys.DEFAULT_EXECUTION_ATTEMPTS);
      if (executionAttempts < this.maxWorkUnitExecutionAttempts) {
        workUnit = createNewWorkUnit(workUnit.getProp(HivePurgerConfigurationKeys.PARTITION_NAME), ++executionAttempts);
        log.info("Revived old Work Unit for partiton " + workUnit.getProp(HivePurgerConfigurationKeys.PARTITION_NAME)
            + " having execution attempt " + workUnit.getProp(HivePurgerConfigurationKeys.EXECUTION_ATTEMPTS));
        workUnitMap.put(workUnit.getProp(HivePurgerConfigurationKeys.PARTITION_NAME), workUnit);
      }
    }
  }

  @Override
  public Extractor getExtractor(WorkUnitState state) {
    return new HivePurgerExtractor(state);
  }

  @Override
  public void shutdown(SourceState state) {
  }

  /**
   * Sets the local watermark, which is a class variable. Local watermark is a complete partition name which act as the starting point for the creation of fresh work units.
   */
  protected void setLowWatermark(SourceState state) {
    this.lowWatermark = getWatermarkFromPreviousWorkUnits(state, HivePurgerConfigurationKeys.HIVE_PURGER_WATERMARK);
    log.info("Setting low watermark for the job: " + this.lowWatermark);
  }

  protected String getLowWatermark() {
    return this.lowWatermark;
  }

  /**
   * Sets Job Watermark in the SourceState which will be copied to all WorkUnitStates. Job Watermark is a complete partition name.
   * During next run of this job, fresh work units will be created starting from this partition.
   */
  protected void setJobWatermark(SourceState state, String watermark) {
    state.setProp(HivePurgerConfigurationKeys.HIVE_PURGER_WATERMARK, watermark);
    log.info("Setting job watermark for the job: " + watermark);
  }

  protected void setTimeStamp() {
    this.timeStamp = Long.toString(System.currentTimeMillis());
  }
}
