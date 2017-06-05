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

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.thrift.TException;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;

import lombok.extern.slf4j.Slf4j;

import gobblin.configuration.State;
import gobblin.data.management.copy.hive.HiveDataset;
import gobblin.data.management.copy.hive.HiveDatasetFinder;
import gobblin.dataset.DatasetsFinder;
import gobblin.hive.HiveMetastoreClientPool;
import gobblin.util.AutoReturnableObject;
import gobblin.util.WriterUtils;
import gobblin.util.reflection.GobblinConstructorUtils;


/**
 * A finder class to find {@link HivePartitionDataset}s.
 *
 * @author adsharma
 */
@Slf4j
public class HivePartitionFinder implements DatasetsFinder<HivePartitionDataset> {
  protected List<HiveDataset> hiveDatasets;
  protected State state;
  private static final Splitter AT_SPLITTER = Splitter.on("@").omitEmptyStrings().trimResults();
  private static Optional<HiveMetastoreClientPool> pool = Optional.<HiveMetastoreClientPool>absent();
  private static final Object lock = new Object();

  public HivePartitionFinder(State state)
      throws IOException {
    this.state = new State(state);
    this.hiveDatasets = getHiveDatasets(WriterUtils.getWriterFs(this.state), this.state);
  }

  private static List<HiveDataset> getHiveDatasets(FileSystem fs, State state)
      throws IOException {
    Preconditions.checkArgument(state.contains(ComplianceConfigurationKeys.COMPLIANCE_DATASET_WHITELIST),
        "Missing required property " + ComplianceConfigurationKeys.COMPLIANCE_DATASET_WHITELIST);
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
    synchronized (lock) {
      List<String> partitionList = AT_SPLITTER.splitToList(completePartitionName);
      Preconditions.checkArgument(partitionList.size() == 3, "Invalid partition name");
      if (!pool.isPresent()) {
        pool = Optional.of(HiveMetastoreClientPool.get(new Properties(),
            Optional.fromNullable(new Properties().getProperty(HiveDatasetFinder.HIVE_METASTORE_URI_KEY))));
      }
      try (AutoReturnableObject<IMetaStoreClient> client = pool.get().getClient()) {
        Table table = new Table(client.get().getTable(partitionList.get(0), partitionList.get(1)));
        Partition partition = new Partition(table,
            client.get().getPartition(partitionList.get(0), partitionList.get(1), partitionList.get(2)));
        return new HivePartitionDataset(partition);
      } catch (TException | HiveException e) {
        throw new IOException(e);
      }
    }
  }

  @Override
  public Path commonDatasetRoot() {
    // Not implemented by this method
    throw new NotImplementedException();
  }
}
