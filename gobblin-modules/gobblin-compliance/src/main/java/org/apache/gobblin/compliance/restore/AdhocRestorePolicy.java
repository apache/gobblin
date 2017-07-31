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
package org.apache.gobblin.compliance.restore;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;

import com.google.common.base.Preconditions;

import org.apache.gobblin.compliance.ComplianceConfigurationKeys;
import org.apache.gobblin.compliance.HivePartitionDataset;
import org.apache.gobblin.compliance.HivePartitionFinder;
import org.apache.gobblin.compliance.utils.ProxyUtils;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.util.HadoopUtils;


/**
 * A restore policy to restore {@link HivePartitionDataset} from a given backup
 *
 * @author adsharma
 */
public class AdhocRestorePolicy extends HivePartitionRestorePolicy {

  public AdhocRestorePolicy(State state) {
    super(state);
  }

  /**
   * @param dataset to restore
   * @return dataset to restore with
   */
  public HivePartitionDataset getDatasetToRestore(HivePartitionDataset dataset)
      throws IOException {
    Preconditions.checkArgument(this.state.contains(ComplianceConfigurationKeys.DATASET_TO_RESTORE),
        "Missing required property " + ComplianceConfigurationKeys.DATASET_TO_RESTORE);
    HivePartitionDataset hivePartitionDataset =
        HivePartitionFinder.findDataset(this.state.getProp(ComplianceConfigurationKeys.DATASET_TO_RESTORE), this.state);
    FileSystem fs = ProxyUtils.getOwnerFs(new State(this.state), hivePartitionDataset.getOwner());
    Preconditions.checkArgument(HadoopUtils.hasContent(fs, hivePartitionDataset.getLocation()),
        "Dataset to restore doesn't have any data");
    return hivePartitionDataset;
  }
}
