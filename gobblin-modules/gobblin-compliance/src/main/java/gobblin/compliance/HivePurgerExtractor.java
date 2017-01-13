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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.ql.metadata.Partition;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;

import gobblin.configuration.WorkUnitState;
import gobblin.data.management.copy.hive.HiveDataset;
import gobblin.data.management.copy.hive.HiveDatasetFinder;
import gobblin.dataset.IterableDatasetFinder;
import gobblin.source.extractor.Extractor;
import gobblin.util.HadoopUtils;
import gobblin.util.reflection.GobblinConstructorUtils;


/**
 * This extractor doesn't extract anything, but is used to instantiate and pass {@link ComplianceRecord}
 * to the converter.
 *
 * @author adsharma
 */
public class HivePurgerExtractor implements Extractor<ComplianceRecordSchema, ComplianceRecord> {
  private static final Splitter AT_SPLITTER = Splitter.on("@").omitEmptyStrings().trimResults();

  private boolean read = false;
  private ComplianceRecord record;

  public HivePurgerExtractor(WorkUnitState state) {
    this.record = createComplianceRecord(state);
  }

  @Override
  public ComplianceRecordSchema getSchema() {
    return new ComplianceRecordSchema();
  }

  /**
   * There is only one record {@link ComplianceRecord} to be read per partition, and must return null
   * after that to indicate end of reading.
   */
  @Override
  public ComplianceRecord readRecord(ComplianceRecord record)
      throws IOException {
    if (read) {
      return null;
    }
    read = true;
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
  public void setRecord(ComplianceRecord record) {
    this.record = record;
  }

  private ComplianceRecord createComplianceRecord(WorkUnitState state) {
    Partition hiveTablePartition =
        getHiveTablePartition(state.getWorkunit().getProp(HivePurgerConfigurationKeys.PARTITION_NAME),
            state.getProperties());
    Map<String, String> partitionParameters = hiveTablePartition.getTable().getParameters();
    Preconditions.checkArgument(state.contains(HivePurgerConfigurationKeys.STAGING_DB_KEY),
        "Missing property " + HivePurgerConfigurationKeys.STAGING_DB_KEY);
    Preconditions.checkArgument(state.contains(HivePurgerConfigurationKeys.STAGING_DIR_KEY),
        "Missing property " + HivePurgerConfigurationKeys.STAGING_DIR_KEY);
    Preconditions.checkArgument(state.contains(HivePurgerConfigurationKeys.COMPLIANCE_IDENTIFIER_KEY),
        "Missing property " + HivePurgerConfigurationKeys.DATASET_DESCRIPTOR_IDENTIFIER);
    Preconditions.checkArgument(state.contains(HivePurgerConfigurationKeys.COMPLIANCE_ID_TABLE_KEY),
        "Missing property " + HivePurgerConfigurationKeys.COMPLIANCE_ID_TABLE_KEY);
    Preconditions.checkArgument(state.contains(HivePurgerConfigurationKeys.DATASET_DESCRIPTOR_IDENTIFIER),
        "Missing property " + HivePurgerConfigurationKeys.DATASET_DESCRIPTOR_IDENTIFIER);
    Preconditions.checkArgument(partitionParameters.containsKey(HivePurgerConfigurationKeys.DATASET_DESCRIPTOR_KEY),
        "Missing table property " + HivePurgerConfigurationKeys.DATASET_DESCRIPTOR_KEY);
    Preconditions.checkArgument(state.contains(HivePurgerConfigurationKeys.HIVE_PURGER_JOB_TIMESTAMP),
        "Missing table property " + HivePurgerConfigurationKeys.HIVE_PURGER_JOB_TIMESTAMP);

    String datasetDescriptorClass = state.getProp(HivePurgerConfigurationKeys.DATASET_DESCRIPTOR_CLASS,
        HivePurgerConfigurationKeys.DEFAULT_DATASET_DESCRIPTOR_CLASS);
    DatasetDescriptor descriptor = GobblinConstructorUtils
        .invokeConstructor(DatasetDescriptor.class, datasetDescriptorClass,
            partitionParameters.get(HivePurgerConfigurationKeys.DATASET_DESCRIPTOR_KEY),
            state.getProp(HivePurgerConfigurationKeys.DATASET_DESCRIPTOR_IDENTIFIER));

    Boolean commit =
        state.getPropAsBoolean(HivePurgerConfigurationKeys.COMMIT_KEY, HivePurgerConfigurationKeys.DEFAULT_COMMIT);
    String stagingDir = state.getProp(HivePurgerConfigurationKeys.STAGING_DIR_KEY);
    String stagingDb = state.getProp(HivePurgerConfigurationKeys.STAGING_DB_KEY);
    String complianceIdentifier = state.getProp(HivePurgerConfigurationKeys.COMPLIANCE_IDENTIFIER_KEY);
    String complianceIdTable = state.getProp(HivePurgerConfigurationKeys.COMPLIANCE_ID_TABLE_KEY);

    ComplianceRecord complianceRecord =
        ComplianceRecord.builder().complianceIdentifier(complianceIdentifier).complianceIdTable(complianceIdTable)
            .commit(commit).stagingDb(stagingDb).stagingDir(stagingDir)
            .datasetComplianceId(descriptor.getComplianceId())
            .timeStamp(state.getProp(HivePurgerConfigurationKeys.HIVE_PURGER_JOB_TIMESTAMP)).build();
    complianceRecord.setHivePartition(hiveTablePartition);
    return complianceRecord;
  }

  /**
   * @throws IOException
   * @Returns Partition from the partition name.
   */
  private Partition getHiveTablePartition(String partitionName, Properties properties) {
    Partition hiveTablePartition = null;
    try {
      properties.setProperty(HivePurgerConfigurationKeys.HIVE_DATASET_WHITELIST, getCompleteTableName(partitionName));
      IterableDatasetFinder<HiveDataset> datasetFinder =
          new HiveDatasetFinder(FileSystem.newInstance(HadoopUtils.newConfiguration()), properties);
      Iterator<HiveDataset> hiveDatasetIterator = datasetFinder.getDatasetsIterator();
      Preconditions.checkArgument(hiveDatasetIterator.hasNext(), "Unable to find table to update from");
      HiveDataset hiveDataset = hiveDatasetIterator.next();
      List<Partition> partitions = hiveDataset.getPartitionsFromDataset();
      Preconditions
          .checkArgument(!partitions.isEmpty(), "No partitions found for " + getCompleteTableName(partitionName));
      for (Partition partition : partitions) {
        if (partition.getCompleteName().equals(partitionName)) {
          hiveTablePartition = partition;
        }
      }
    } catch (IOException e) {
      Throwables.propagate(e);
    }
    Preconditions.checkNotNull(hiveTablePartition, "Cannot find the required partition " + partitionName);
    return hiveTablePartition;
  }

  /**
   * @param completePartitionName String with dbName.tableName.partitionName
   * @return String with dbName.tableName
   */
  private String getCompleteTableName(String completePartitionName) {
    List<String> list = AT_SPLITTER.splitToList(completePartitionName);
    Preconditions.checkArgument(list.size() == 3, "Incorrect partition name format " + completePartitionName);
    return list.get(0) + "." + list.get(1);
  }
}
