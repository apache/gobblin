/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */
package gobblin.compliance;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.ql.metadata.Partition;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;

import gobblin.configuration.WorkUnitState;
import gobblin.data.management.copy.hive.HiveDataset;
import gobblin.data.management.copy.hive.HiveDatasetFinder;
import gobblin.dataset.IterableDatasetFinder;
import gobblin.util.HadoopUtils;
import gobblin.util.reflection.GobblinConstructorUtils;

/**
 * This class is used to pass information from extractor to converter to writer.
 * It contains all information needed to successfully purge partitions.
 *
 * @author adsharma
 */
public class HivePurgerPartitionRecord {
  private String partitionName;
  private String stagingDir;
  private String stagingDb;
  private String complianceIdTable;
  private String complianceIdentifier;

  private List<String> purgeQueries;

  private Boolean commit;

  private Partition hiveTablePartition;

  private final Splitter AT_SPLITTER = Splitter.on("@").omitEmptyStrings().trimResults();
  private WorkUnitState state;

  private DatasetDescriptor descriptor;

  public HivePurgerPartitionRecord(String partitionName, WorkUnitState state)
      throws IOException {
    this.purgeQueries = new ArrayList<>();
    this.partitionName = partitionName;
    this.state = state;
    setHiveTablePartition();
    setComplianceIdTable(state);
    setStagingDir(state);
    setStagingDb(state);
    setCommit(state);
    setDatasetDescriptor(state);
    setComplianceIdentifier(state);
  }

  public WorkUnitState getState() {
    return this.state;
  }

  public void setDatasetDescriptor(WorkUnitState state) {
    Map<String, String> partitionParameters = this.hiveTablePartition.getTable().getParameters();
    Preconditions.checkArgument(partitionParameters.containsKey(HivePurgerConfigurationKeys.DATASET_DESCRIPTOR_KEY),
        "Missing table property " + HivePurgerConfigurationKeys.DATASET_DESCRIPTOR_KEY);
    Preconditions.checkArgument(state.contains(HivePurgerConfigurationKeys.DATASET_DESCRIPTOR_IDENTIFIER),
        "Missing property " + HivePurgerConfigurationKeys.DATASET_DESCRIPTOR_IDENTIFIER);
    String datasetDescriptorClass = state.getProp(HivePurgerConfigurationKeys.DATASET_DESCRIPTOR_CLASS,
        HivePurgerConfigurationKeys.DEFAULT_DATASET_DESCRIPTOR_CLASS);
    this.descriptor = GobblinConstructorUtils.invokeConstructor(DatasetDescriptor.class, datasetDescriptorClass,
        partitionParameters.get(HivePurgerConfigurationKeys.DATASET_DESCRIPTOR_KEY),
        state.getProp(HivePurgerConfigurationKeys.DATASET_DESCRIPTOR_IDENTIFIER));
  }

  public DatasetDescriptor getDatasetDescriptor() {
    return this.descriptor;
  }

  public void setComplianceIdentifier(WorkUnitState state) {
    Preconditions.checkArgument(state.contains(HivePurgerConfigurationKeys.COMPLIANCE_ID_IDENTIFIER_KEY),
        "Missing property " + HivePurgerConfigurationKeys.DATASET_DESCRIPTOR_IDENTIFIER);
    this.complianceIdentifier = state.getProp(HivePurgerConfigurationKeys.COMPLIANCE_ID_IDENTIFIER_KEY);
  }

  public String getComplianceIdentifier() {
    return this.complianceIdentifier;
  }

  public Partition getHiveTablePartition() {
    return this.hiveTablePartition;
  }

  /**
   * Get {@link Partition} from the partition name.
   * @throws IOException
   */
  public void setHiveTablePartition()
      throws IOException {
    this.hiveTablePartition = null;
    Properties properties = new Properties();
    properties.putAll(state.getProperties());

    properties
        .setProperty(HivePurgerConfigurationKeys.HIVE_DATASET_WHITELIST, getCompleteTableName(this.partitionName));
    IterableDatasetFinder<HiveDataset> datasetFinder =
        new HiveDatasetFinder(FileSystem.newInstance(HadoopUtils.newConfiguration()), properties);
    Iterator<HiveDataset> hiveDatasetIterator = datasetFinder.getDatasetsIterator();
    Preconditions.checkArgument(hiveDatasetIterator.hasNext(), "Unable to find table to update from");
    HiveDataset hiveDataset = hiveDatasetIterator.next();
    List<Partition> partitions = DatasetUtils.getPartitionsFromDataset(hiveDataset);

    Preconditions
        .checkArgument(!partitions.isEmpty(), "No partitions found for " + getCompleteTableName(this.partitionName));

    for (Partition partition : partitions) {
      if (partition.getCompleteName().equals(this.partitionName)) {
        this.hiveTablePartition = partition;
      }
    }
    Preconditions
        .checkNotNull(this.hiveTablePartition, "Cannot find the required partition " + getCompletePartitionName());
  }

  public void addPurgeQueries(List<String> purgeQueries) {
    this.purgeQueries.addAll(purgeQueries);
  }

  public List<String> getPurgeQueries() {
    return this.purgeQueries;
  }

  /**
   * @return String with dbName.tableName.partitionName
   */
  public String getCompletePartitionName() {
    return this.partitionName;
  }

  /**
   * @param completePartitionName
   * @return String with dbName.tableName
   */
  public String getCompleteTableName(String completePartitionName) {
    List<String> list = this.AT_SPLITTER.splitToList(completePartitionName);
    Preconditions.checkArgument(list.size() == 3, "Incorrect partition name format " + completePartitionName);
    return list.get(0) + "." + list.get(1);
  }

  public void setComplianceIdTable(WorkUnitState state) {
    Preconditions.checkArgument(state.contains(HivePurgerConfigurationKeys.COMPLIANCE_ID_TABLE_KEY),
        "Missing property " + HivePurgerConfigurationKeys.COMPLIANCE_ID_TABLE_KEY);
    this.complianceIdTable = state.getProp(HivePurgerConfigurationKeys.COMPLIANCE_ID_TABLE_KEY);
  }

  public String getComplianceIdTable() {
    return this.complianceIdTable;
  }

  public void setStagingDir(WorkUnitState state) {
    Preconditions.checkArgument(state.contains(HivePurgerConfigurationKeys.STAGING_DIR_KEY),
        "Missing property " + HivePurgerConfigurationKeys.STAGING_DIR_KEY);
    String stagingDir = state.getProp(HivePurgerConfigurationKeys.STAGING_DIR_KEY);
    this.stagingDir = stagingDir;
    if (!stagingDir.endsWith("/")) {
      this.stagingDir = stagingDir + "/";
    }
  }

  public String getStagingDir() {
    return this.stagingDir;
  }

  public void setStagingDb(WorkUnitState state) {
    Preconditions.checkArgument(state.contains(HivePurgerConfigurationKeys.STAGING_DB_KEY),
        "Missing property " + HivePurgerConfigurationKeys.STAGING_DB_KEY);
    this.stagingDb = state.getProp(HivePurgerConfigurationKeys.STAGING_DB_KEY);
  }

  public String getStagingDb() {
    return this.stagingDb;
  }

  public void setCommit(WorkUnitState state) {
    this.commit =
        state.getPropAsBoolean(HivePurgerConfigurationKeys.COMMIT_KEY, HivePurgerConfigurationKeys.DEFAULT_COMMIT);
  }

  public Boolean shouldCommit() {
    return this.commit;
  }

  public String getFinalPartitionLocation() {
    return DatasetUtils.getFinalPartitionLocation(this);
  }

  public String getStagingPartitionLocation() {
    return DatasetUtils.getStagingPartitionLocation(this);
  }
}

