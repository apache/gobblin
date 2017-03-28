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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;

import lombok.extern.slf4j.Slf4j;

import gobblin.compliance.ComplianceConfigurationKeys;
import gobblin.compliance.ComplianceEvents;
import gobblin.compliance.DatasetUtils;
import gobblin.compliance.HivePartitionDataset;
import gobblin.compliance.HivePartitionFinder;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.dataset.DatasetsFinder;
import gobblin.instrumented.Instrumented;
import gobblin.metrics.MetricContext;
import gobblin.metrics.event.EventSubmitter;
import gobblin.publisher.DataPublisher;
import gobblin.source.workunit.WorkUnit;
import gobblin.util.reflection.GobblinConstructorUtils;


/**
 * The Publisher moves COMMITTED WorkUnitState to SUCCESSFUL, otherwise FAILED.
 *
 * @author adsharma
 */
@Slf4j
public class HivePurgerPublisher extends DataPublisher {
  protected List<HivePartitionDataset> datasets = new ArrayList<>();
  protected MetricContext metricContext;
  protected EventSubmitter eventSubmitter;
  protected DatasetsFinder datasetFinder;

  public HivePurgerPublisher(State state) {
    super(state);
    this.metricContext = Instrumented.getMetricContext(state, this.getClass());
    this.eventSubmitter = new EventSubmitter.Builder(this.metricContext, ComplianceEvents.NAMESPACE).
        build();
    String datasetFinderClass = state.getProp(ComplianceConfigurationKeys.GOBBLIN_COMPLIANCE_DATASET_FINDER_CLASS,
        HivePartitionFinder.class.getName());
    this.datasetFinder = GobblinConstructorUtils.invokeConstructor(DatasetsFinder.class, datasetFinderClass, state);
    try {
      this.datasets = this.datasetFinder.findDatasets();
    } catch (IOException e) {
      Throwables.propagate(e);
    }
  }

  public void initialize() {
  }

  @Override
  public void publishData(Collection<? extends WorkUnitState> states) {
    for (WorkUnitState state : states) {
      if (state.getWorkingState() == WorkUnitState.WorkingState.SUCCESSFUL) {
        state.setWorkingState(WorkUnitState.WorkingState.COMMITTED);
        submitEvent(state, ComplianceEvents.Purger.WORKUNIT_COMMITTED);
      } else {
        state.setWorkingState(WorkUnitState.WorkingState.FAILED);
        submitEvent(state, ComplianceEvents.Purger.WORKUNIT_FAILED);
      }
    }
  }

  private void submitEvent(WorkUnitState state, String name) {
    WorkUnit workUnit = state.getWorkunit();
    Map<String, String> metadata = new HashMap<>();
    metadata.put(ComplianceConfigurationKeys.WORKUNIT_RECORDSREAD, state.getProp(ComplianceConfigurationKeys.NUM_ROWS));
    metadata.put(ComplianceConfigurationKeys.WORKUNIT_BYTESREAD,
        getDataSize(workUnit.getProp(ComplianceConfigurationKeys.RAW_DATA_SIZE),
            workUnit.getProp(ComplianceConfigurationKeys.TOTAL_DATA_SIZE)));
    Optional<HivePartitionDataset> dataset =
        DatasetUtils.findDataset(workUnit.getProp(ComplianceConfigurationKeys.PARTITION_NAME), this.datasets);
    if (!dataset.isPresent()) {
      return;
    }

    HivePartitionDataset hivePartitionDataset = dataset.get();

    metadata.put(ComplianceConfigurationKeys.WORKUNIT_RECORDSWRITTEN, DatasetUtils
        .getPropertyFromDataset(hivePartitionDataset, ComplianceConfigurationKeys.NUM_ROWS,
            ComplianceConfigurationKeys.DEFAULT_NUM_ROWS));
    metadata.put(ComplianceConfigurationKeys.WORKUNIT_BYTESWRITTEN, getDataSize(DatasetUtils
        .getPropertyFromDataset(hivePartitionDataset, ComplianceConfigurationKeys.RAW_DATA_SIZE,
            ComplianceConfigurationKeys.DEFAULT_RAW_DATA_SIZE), DatasetUtils
        .getPropertyFromDataset(hivePartitionDataset, ComplianceConfigurationKeys.TOTAL_DATA_SIZE,
            ComplianceConfigurationKeys.DEFAULT_TOTAL_DATA_SIZE)));

    metadata.put(ComplianceConfigurationKeys.PARTITION_NAME, hivePartitionDataset.datasetURN());

    this.eventSubmitter.submit(name, metadata);
  }

  private String getDataSize(String rawDataSizeString, String totalDataSizeString) {
    int rawDataSize = Integer.parseInt(rawDataSizeString);
    int totalDataSize = Integer.parseInt(totalDataSizeString);
    int dataSize = totalDataSize;
    if (totalDataSize <= 0) {
      dataSize = rawDataSize;
    }
    return Integer.toString(dataSize);
  }

  public void publishMetadata(Collection<? extends WorkUnitState> states) {
  }

  @Override
  public void close() {
  }
}
