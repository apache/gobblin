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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;

import com.google.common.base.Preconditions;

import org.apache.gobblin.compliance.ComplianceConfigurationKeys;
import org.apache.gobblin.compliance.HivePartitionDataset;
import org.apache.gobblin.compliance.HivePartitionVersion;
import org.apache.gobblin.compliance.HivePartitionVersionFinder;
import org.apache.gobblin.compliance.utils.ProxyUtils;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.util.HadoopUtils;
import org.apache.gobblin.util.WriterUtils;


/**
 * Last Known Good (LKG) restore policy
 *
 * @author adsharma
 */
public class LKGRestorePolicy extends HivePartitionRestorePolicy {
  public LKGRestorePolicy(State state) {
    super(state);
  }

  /**
   * @param dataset to restore
   * @return most recent restorable dataset
   */
  public HivePartitionDataset getDatasetToRestore(HivePartitionDataset dataset)
      throws IOException {
    List<String> patterns = new ArrayList<>();
    patterns.add(getCompleteTableName(dataset) + ComplianceConfigurationKeys.BACKUP);
    HivePartitionVersionFinder finder =
        new HivePartitionVersionFinder(WriterUtils.getWriterFs(new State(this.state)), this.state, patterns);

    List<HivePartitionVersion> versions = new ArrayList<>(finder.findDatasetVersions(dataset));
    Preconditions.checkArgument(!versions.isEmpty(), "No versions to restore dataset " + dataset.datasetURN());
    List<HivePartitionVersion> nonRestorableVersions = new ArrayList<>();

    for (HivePartitionVersion version : versions) {
      if (!isRestorable(dataset, version)) {
        nonRestorableVersions.add(version);
      }
    }
    versions.removeAll(nonRestorableVersions);
    Preconditions.checkArgument(!versions.isEmpty(), "No versions to restore dataset " + dataset.datasetURN());
    Collections.sort(versions);
    // return the most recent restorable version
    return new HivePartitionDataset(versions.get(0));
  }

  /**
   * A version is called restorable if it can be used to restore dataset.
   *
   * If a version is pointing to same data location as of the dataset, then it can't be used for restoring
   * If a version is pointing to an empty data location, then it can't be used for restoring
   */
  private boolean isRestorable(HivePartitionDataset dataset, HivePartitionVersion version)
      throws IOException {
    if (version.getLocation().toString().equalsIgnoreCase(dataset.getLocation().toString())) {
      return false;
    }
    FileSystem fs = ProxyUtils.getOwnerFs(new State(this.state), version.getOwner());
    if (!HadoopUtils.hasContent(fs, version.getLocation())) {
      return false;
    }
    return true;
  }

  private String getCompleteTableName(HivePartitionDataset dataset) {
    return dataset.getDbName() + ComplianceConfigurationKeys.DBNAME_SEPARATOR + dataset.getTableName();
  }
}
