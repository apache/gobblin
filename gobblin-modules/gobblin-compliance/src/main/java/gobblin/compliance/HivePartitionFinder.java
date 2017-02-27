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
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.metadata.Partition;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;

import gobblin.configuration.State;
import gobblin.data.management.copy.hive.HiveDataset;
import gobblin.data.management.copy.hive.HiveDatasetFinder;
import gobblin.dataset.DatasetsFinder;
import gobblin.util.WriterUtils;
import gobblin.util.reflection.GobblinConstructorUtils;


/**
 * A finder class to find {@link HivePartitionDataset}s.
 *
 * @author adsharma
 */
public class HivePartitionFinder implements DatasetsFinder<HivePartitionDataset> {
  protected List<HiveDataset> hiveDatasets;
  protected State state;
  private static final Splitter AT_SPLITTER = Splitter.on("@").omitEmptyStrings().trimResults();

  public HivePartitionFinder(State state)
      throws IOException {
    this.state = new State(state);
    this.hiveDatasets = getHiveDatasets(WriterUtils.getWriterFs(this.state), this.state);
  }

  private static List<HiveDataset> getHiveDatasets(FileSystem fs, State state)
      throws IOException {
    Preconditions.checkArgument(state.contains(ComplianceConfigurationKeys.COMPLIANCE_DATASET_WHITELIST));
    Properties prop = new Properties();
    prop.setProperty(ComplianceConfigurationKeys.HIVE_DATASET_WHITELIST,
        state.getProp(ComplianceConfigurationKeys.COMPLIANCE_DATASET_WHITELIST));
    HiveDatasetFinder finder = new HiveDatasetFinder(fs, prop);
    return finder.findDatasets();
  }

  /**
   * Will find all datasets according to whitelist, except the backup, trash and staging tables.
   */
  @Override
  public List<HivePartitionDataset> findDatasets()
      throws IOException {
    List<HivePartitionDataset> list = new ArrayList<>();
    for (HiveDataset hiveDataset : this.hiveDatasets) {
      for (Partition partition : hiveDataset.getPartitionsFromDataset()) {
        list.add(new HivePartitionDataset(partition));
      }
    }
    String selectionPolicyString = this.state.getProp(ComplianceConfigurationKeys.DATASET_SELECTION_POLICY_CLASS,
        ComplianceConfigurationKeys.DEFAULT_DATASET_SELECTION_POLICY_CLASS);
    Policy<HivePartitionDataset> selectionPolicy =
        GobblinConstructorUtils.invokeConstructor(Policy.class, selectionPolicyString);
    return selectionPolicy.selectedList(list);
  }

  public static HivePartitionDataset findDataset(String completePartitionName, State prop)
      throws IOException {
    Partition hiveTablePartition = null;
    State state = new State(prop);
    state.setProp(ComplianceConfigurationKeys.HIVE_DATASET_WHITELIST,
        getCompleteTableNameForWhitelist(completePartitionName));
    List<HiveDataset> hiveDatasets = getHiveDatasets(WriterUtils.getWriterFs(state), state);
    Preconditions.checkArgument(hiveDatasets.size() == 1, "Cannot find required table for " + completePartitionName);
    List<Partition> partitions = hiveDatasets.get(0).getPartitionsFromDataset();
    Preconditions.checkArgument(!partitions.isEmpty(),
        "No partitions found for " + getCompleteTableNameForWhitelist(completePartitionName));
    for (Partition partition : partitions) {
      if (partition.getCompleteName().equals(completePartitionName)) {
        hiveTablePartition = partition;
        break;
      }
    }
    Preconditions.checkNotNull(hiveTablePartition, "Cannot find the required partition " + completePartitionName);
    return new HivePartitionDataset(hiveTablePartition);
  }

  private static String getCompleteTableNameForWhitelist(String completePartitionName) {
    List<String> list = AT_SPLITTER.splitToList(completePartitionName);
    Preconditions.checkArgument(list.size() == 3, "Incorrect partition name format " + completePartitionName);
    return list.get(0) + "." + list.get(1);
  }

  @Override
  public Path commonDatasetRoot() {
    // Not implemented by this method
    return null;
  }
}
