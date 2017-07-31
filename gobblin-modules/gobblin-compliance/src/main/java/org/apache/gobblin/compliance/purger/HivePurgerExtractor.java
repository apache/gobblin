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
package org.apache.gobblin.compliance.purger;

import java.io.IOException;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import org.apache.gobblin.compliance.ComplianceConfigurationKeys;
import org.apache.gobblin.compliance.DatasetDescriptor;
import org.apache.gobblin.compliance.HivePartitionDataset;
import org.apache.gobblin.compliance.HivePartitionFinder;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.source.extractor.Extractor;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;


/**
 * This extractor doesn't extract anything, but is used to instantiate and pass {@link PurgeableHivePartitionDataset}
 * to the converter.
 *
 * @author adsharma
 */
public class HivePurgerExtractor implements Extractor<PurgeableHivePartitionDatasetSchema, PurgeableHivePartitionDataset> {
  private PurgeableHivePartitionDataset record;
  private State state;
  private boolean read;

  public HivePurgerExtractor(WorkUnitState state)
      throws IOException {
    this.read = false;
    this.state = new State(state);
  }

  @Override
  public PurgeableHivePartitionDatasetSchema getSchema() {
    return new PurgeableHivePartitionDatasetSchema();
  }

  /**
   * There is only one record {@link PurgeableHivePartitionDataset} to be read per partition, and must return null
   * after that to indicate end of reading.
   */
  @Override
  public PurgeableHivePartitionDataset readRecord(PurgeableHivePartitionDataset record)
      throws IOException {
    if (this.read) {
      return null;
    }
    this.read = true;
    if (this.record == null) {
      this.record = createPurgeableHivePartitionDataset(this.state);
    }
    return this.record;
  }

  @Override
  public long getExpectedRecordCount() {
    return 1;
  }

  /**
   * Watermark is not managed by this extractor.
   */
  @Override
  public long getHighWatermark() {
    return 0;
  }

  @Override
  public void close()
      throws IOException {
  }

  @VisibleForTesting
  public void setRecord(PurgeableHivePartitionDataset record) {
    this.record = record;
  }

  private PurgeableHivePartitionDataset createPurgeableHivePartitionDataset(State state)
      throws IOException {
    HivePartitionDataset hivePartitionDataset =
        HivePartitionFinder.findDataset(state.getProp(ComplianceConfigurationKeys.PARTITION_NAME), state);
    Preconditions.checkArgument(state.contains(ComplianceConfigurationKeys.COMPLIANCEID_KEY),
        "Missing property " + ComplianceConfigurationKeys.COMPLIANCEID_KEY);
    Preconditions.checkArgument(state.contains(ComplianceConfigurationKeys.COMPLIANCE_ID_TABLE_KEY),
        "Missing property " + ComplianceConfigurationKeys.COMPLIANCE_ID_TABLE_KEY);
    Preconditions.checkArgument(state.contains(ComplianceConfigurationKeys.TIMESTAMP),
        "Missing table property " + ComplianceConfigurationKeys.TIMESTAMP);

    Boolean simulate = state.getPropAsBoolean(ComplianceConfigurationKeys.COMPLIANCE_JOB_SIMULATE,
        ComplianceConfigurationKeys.DEFAULT_COMPLIANCE_JOB_SIMULATE);
    String complianceIdentifier = state.getProp(ComplianceConfigurationKeys.COMPLIANCEID_KEY);
    String complianceIdTable = state.getProp(ComplianceConfigurationKeys.COMPLIANCE_ID_TABLE_KEY);
    String timeStamp = state.getProp(ComplianceConfigurationKeys.TIMESTAMP);
    Boolean specifyPartitionFormat = state.getPropAsBoolean(ComplianceConfigurationKeys.SPECIFY_PARTITION_FORMAT,
        ComplianceConfigurationKeys.DEFAULT_SPECIFY_PARTITION_FORMAT);

    State datasetState = new State();
    datasetState.addAll(state.getProperties());
    PurgeableHivePartitionDataset dataset = new PurgeableHivePartitionDataset(hivePartitionDataset);
    dataset.setComplianceId(complianceIdentifier);
    dataset.setComplianceIdTable(complianceIdTable);
    dataset.setComplianceField(getComplianceField(state, hivePartitionDataset));
    dataset.setTimeStamp(timeStamp);
    dataset.setState(datasetState);
    dataset.setSimulate(simulate);
    dataset.setSpecifyPartitionFormat(specifyPartitionFormat);
    return dataset;
  }

  private String getComplianceField(State state, HivePartitionDataset dataset) {
    Map<String, String> partitionParameters = dataset.getTableParams();
    Preconditions.checkArgument(partitionParameters.containsKey(ComplianceConfigurationKeys.DATASET_DESCRIPTOR_KEY),
        "Missing table property " + ComplianceConfigurationKeys.DATASET_DESCRIPTOR_KEY);
    String datasetDescriptorClass = state.getProp(ComplianceConfigurationKeys.DATASET_DESCRIPTOR_CLASS,
        ComplianceConfigurationKeys.DEFAULT_DATASET_DESCRIPTOR_CLASS);
    DatasetDescriptor descriptor = GobblinConstructorUtils
        .invokeConstructor(DatasetDescriptor.class, datasetDescriptorClass,
            partitionParameters.get(ComplianceConfigurationKeys.DATASET_DESCRIPTOR_KEY),
            Optional.fromNullable(state.getProp(ComplianceConfigurationKeys.DATASET_DESCRIPTOR_FIELDPATH)));
    return descriptor.getComplianceField();
  }
}
