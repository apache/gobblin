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
package gobblin.compliance.restore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import gobblin.compliance.ComplianceConfigurationKeys;
import gobblin.compliance.HivePartitionDataset;
import gobblin.compliance.HivePartitionVersion;
import gobblin.compliance.retention.HivePartitionRetentionVersionFinder;
import gobblin.compliance.utils.ProxyUtils;
import gobblin.configuration.State;
import gobblin.util.HadoopUtils;
import gobblin.util.WriterUtils;


/**
 * Last Known Good (LKG) restore policy
 *
 * @author adsharma
 */
public class LKGRestorePolicy extends HivePartitionRestorePolicy {
  public LKGRestorePolicy(State state) {
    super(state);
  }

  public HivePartitionDataset getDatasetToRestore(HivePartitionDataset dataset)
      throws IOException {
    List<String> patterns = new ArrayList<>();
    patterns.add(getCompleteTableName(dataset) + ComplianceConfigurationKeys.BACKUP);
    HivePartitionRetentionVersionFinder finder =
        new HivePartitionRetentionVersionFinder(WriterUtils.getWriterFs(new State(this.state)), this.state, patterns);
    List<HivePartitionVersion> versions = new ArrayList<>(finder.findDatasetVersions(dataset));
    Preconditions.checkArgument(!versions.isEmpty(), "No versions to restore dataset " + dataset.datasetURN());
    List<HivePartitionVersion> nonRestorableVersions = new ArrayList<>();
    for (HivePartitionVersion version : versions) {
      if (version.getLocation().toString().equalsIgnoreCase(dataset.getLocation().toString())) {
        nonRestorableVersions.add(version);
      }
      Optional<String> owner = version.getOwner();
      FileSystem fs = ProxyUtils.getOwnerFs(new State(this.state), owner);
      if (!HadoopUtils.hasContent(fs, version.getLocation())) {
        nonRestorableVersions.add(version);
      }
    }
    versions.removeAll(nonRestorableVersions);
    Collections.sort(versions);
    return new HivePartitionDataset(versions.get(0));
  }

  private String getCompleteTableName(HivePartitionDataset dataset) {
    return dataset.getDbName() + ComplianceConfigurationKeys.DBNAME_SEPARATOR + dataset.getTableName();
  }
}
