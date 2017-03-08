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
package gobblin.compliance.purger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;

import lombok.extern.slf4j.Slf4j;

import gobblin.compliance.ComplianceConfigurationKeys;
import gobblin.compliance.HivePartitionDataset;
import gobblin.compliance.HivePartitionFinder;
import gobblin.compliance.utils.ProxyUtils;
import gobblin.configuration.SourceState;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.dataset.DatasetsFinder;
import gobblin.source.Source;
import gobblin.source.extractor.Extractor;
import gobblin.source.workunit.WorkUnit;
import gobblin.util.reflection.GobblinConstructorUtils;


/**
 * This class creates {@link WorkUnit}s and assigns exactly one partition to each of them.
 * It iterates over all Hive Tables specified via whitelist and blacklist, list all partitions, and create
 * workunits.
 *
 * It revive {@link WorkUnit}s if their execution attempts are not exhausted.
 *
 * @author adsharma
 */
@Slf4j
public class HivePurgerSource implements Source {
  protected DatasetsFinder datasetFinder;
  protected Map<String, WorkUnit> workUnitMap = new HashMap<>();
  protected int maxWorkUnitExecutionAttempts;
  protected int maxWorkUnits;
  protected int workUnitsCreatedCount = 0;
  protected String lowWatermark;
  protected String timeStamp;
  protected PurgePolicy policy;
  protected boolean shouldProxy;

  // These datasets are lexicographically sorted by their name
  protected List<HivePartitionDataset> datasets = new ArrayList<>();

  @VisibleForTesting
  protected void initialize(SourceState state)
      throws IOException {
    setTimeStamp();
    this.setLowWatermark(state);
    this.maxWorkUnits = state
        .getPropAsInt(ComplianceConfigurationKeys.MAX_WORKUNITS_KEY, ComplianceConfigurationKeys.DEFAULT_MAX_WORKUNITS);
    this.maxWorkUnitExecutionAttempts = state
        .getPropAsInt(ComplianceConfigurationKeys.MAX_WORKUNIT_EXECUTION_ATTEMPTS_KEY,
            ComplianceConfigurationKeys.DEFAULT_MAX_WORKUNIT_EXECUTION_ATTEMPTS);
    // TODO: Event submitter and metrics will be added later
    String datasetFinderClass = state.getProp(ComplianceConfigurationKeys.GOBBLIN_COMPLIANCE_DATASET_FINDER_CLASS,
        HivePartitionFinder.class.getName());
    this.datasetFinder = GobblinConstructorUtils.invokeConstructor(DatasetsFinder.class, datasetFinderClass, state);
    populateDatasets();
    String policyClass =
        state.getProp(ComplianceConfigurationKeys.PURGE_POLICY_CLASS, HivePurgerPolicy.class.getName());
    this.policy = GobblinConstructorUtils.invokeConstructor(PurgePolicy.class, policyClass, this.lowWatermark);
    this.shouldProxy = state.getPropAsBoolean(ComplianceConfigurationKeys.GOBBLIN_COMPLIANCE_SHOULD_PROXY,
        ComplianceConfigurationKeys.GOBBLIN_COMPLIANCE_DEFAULT_SHOULD_PROXY);
    if (!this.shouldProxy) {
      return;
    }
    // cancel tokens
    try {
      ProxyUtils.cancelTokens(new State(state));
    } catch (InterruptedException | TException e) {
      throw new IOException(e);
    }
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

  protected WorkUnit createNewWorkUnit(String partitionName, int executionAttempts) {
    WorkUnit workUnit = WorkUnit.createEmpty();
    workUnit.setProp(ComplianceConfigurationKeys.PARTITION_NAME, partitionName);
    workUnit.setProp(ComplianceConfigurationKeys.EXECUTION_ATTEMPTS, executionAttempts);
    workUnit.setProp(ComplianceConfigurationKeys.TIMESTAMP, this.timeStamp);
    workUnit.setProp(ComplianceConfigurationKeys.GOBBLIN_COMPLIANCE_SHOULD_PROXY, this.shouldProxy);
    return workUnit;
  }

  protected WorkUnit createNewWorkUnit(String partitionName) {
    return createNewWorkUnit(partitionName, ComplianceConfigurationKeys.DEFAULT_EXECUTION_ATTEMPTS);
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
    for (HivePartitionDataset dataset : this.datasets) {
      Optional<String> owner = dataset.getOwner();
      if (workUnitsExceeded()) {
        log.info("Workunits exceeded");
        setJobWatermark(state, dataset.datasetURN());
        return;
      }
      if (!this.policy.shouldPurge(dataset)) {
        continue;
      }
      WorkUnit workUnit = createNewWorkUnit(dataset.datasetURN());
      log.info("Created new work unit with partition " + workUnit.getProp(ComplianceConfigurationKeys.PARTITION_NAME));
      this.workUnitMap.put(workUnit.getProp(ComplianceConfigurationKeys.PARTITION_NAME), workUnit);
      this.workUnitsCreatedCount++;
    }
    if (!state.contains(ComplianceConfigurationKeys.HIVE_PURGER_WATERMARK)) {
      this.setJobWatermark(state, ComplianceConfigurationKeys.NO_PREVIOUS_WATERMARK);
    }
  }

  protected boolean workUnitsExceeded() {
    return !(this.workUnitsCreatedCount < this.maxWorkUnits);
  }

  /**
   * Find all datasets on the basis on whitelist and blacklist, and then add them in a list in lexicographical order.
   */
  protected void populateDatasets()
      throws IOException {
    this.datasets = this.datasetFinder.findDatasets();
    sortHiveDatasets(datasets);
  }

  /**
   * Sort all HiveDatasets on the basis of complete name ie dbName.tableName
   */
  protected List<HivePartitionDataset> sortHiveDatasets(List<HivePartitionDataset> datasets) {
    Collections.sort(datasets, new Comparator<HivePartitionDataset>() {
      @Override
      public int compare(HivePartitionDataset o1, HivePartitionDataset o2) {
        return o1.datasetURN().compareTo(o2.datasetURN());
      }
    });
    return datasets;
  }

  /**
   * Add failed work units in a workUnitMap with partition name as Key.
   * New work units are created using required configuration from the old work unit.
   */
  protected void createWorkunitsFromPreviousState(SourceState state) {
    if (this.lowWatermark.equalsIgnoreCase(ComplianceConfigurationKeys.NO_PREVIOUS_WATERMARK)) {
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
      Preconditions.checkArgument(workUnit.contains(ComplianceConfigurationKeys.PARTITION_NAME),
          "Older WorkUnit doesn't contain property partition name.");
      int executionAttempts = workUnit.getPropAsInt(ComplianceConfigurationKeys.EXECUTION_ATTEMPTS,
          ComplianceConfigurationKeys.DEFAULT_EXECUTION_ATTEMPTS);
      if (executionAttempts < this.maxWorkUnitExecutionAttempts) {
        workUnit = createNewWorkUnit(workUnit.getProp(ComplianceConfigurationKeys.PARTITION_NAME), ++executionAttempts);
        log.info("Revived old Work Unit for partiton " + workUnit.getProp(ComplianceConfigurationKeys.PARTITION_NAME)
            + " having execution attempt " + workUnit.getProp(ComplianceConfigurationKeys.EXECUTION_ATTEMPTS));
        workUnitMap.put(workUnit.getProp(ComplianceConfigurationKeys.PARTITION_NAME), workUnit);
      }
    }
  }

  protected void setTimeStamp() {
    this.timeStamp = Long.toString(System.currentTimeMillis());
  }

  @Override
  public Extractor getExtractor(WorkUnitState state)
      throws IOException {
    return new HivePurgerExtractor(state);
  }

  @Override
  public void shutdown(SourceState state) {
  }

  /**
   * Sets the local watermark, which is a class variable. Local watermark is a complete partition name which act as the starting point for the creation of fresh work units.
   */
  protected void setLowWatermark(SourceState state) {
    this.lowWatermark = getWatermarkFromPreviousWorkUnits(state, ComplianceConfigurationKeys.HIVE_PURGER_WATERMARK);
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
    state.setProp(ComplianceConfigurationKeys.HIVE_PURGER_WATERMARK, watermark);
    log.info("Setting job watermark for the job: " + watermark);
  }

  /**
   * Fetches the value of a watermark given its key from the previous run.
   */
  protected static String getWatermarkFromPreviousWorkUnits(SourceState state, String watermark) {
    if (state.getPreviousWorkUnitStates().isEmpty()) {
      return ComplianceConfigurationKeys.NO_PREVIOUS_WATERMARK;
    }
    return state.getPreviousWorkUnitStates().get(0)
        .getProp(watermark, ComplianceConfigurationKeys.NO_PREVIOUS_WATERMARK);
  }
}
