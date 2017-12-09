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
package org.apache.gobblin.compliance.retention;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.gobblin.dataset.FileSystemDataset;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.metadata.Partition;

import com.google.common.base.Preconditions;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.compliance.ComplianceConfigurationKeys;
import org.apache.gobblin.compliance.HivePartitionDataset;
import org.apache.gobblin.compliance.HivePartitionVersion;
import org.apache.gobblin.compliance.HivePartitionVersionFinder;
import org.apache.gobblin.compliance.HivePartitionVersionPolicy;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.data.management.retention.dataset.CleanableDataset;
import org.apache.gobblin.data.management.retention.version.VersionCleaner;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;


/**
 * This class is a Cleanable representation of {@link HivePartitionDataset}.
 * This class implements the clean method which will be called for each dataset
 *
 * @author adsharma
 */
@Slf4j
public class CleanableHivePartitionDataset extends HivePartitionDataset implements CleanableDataset, FileSystemDataset {
  private FileSystem fs;
  private State state;

  public CleanableHivePartitionDataset(Partition partition, FileSystem fs, State state) {
    super(partition);
    this.fs = fs;
    this.state = new State(state);
  }

  public CleanableHivePartitionDataset(HivePartitionDataset hivePartitionDataset, FileSystem fs, State state) {
    super(hivePartitionDataset);
    this.fs = fs;
    this.state = new State(state);
  }

  @Override
  public Path datasetRoot() {
    return this.getLocation();
  }

  /**
   * This method uses {@link HivePartitionVersionFinder} to list out versions
   * corresponding to this dataset. It will then filter out versions using {@link HivePartitionVersionPolicy}.
   *
   * For each version there will be a corresponding {@link VersionCleaner} which will clean the version.
   */
  @Override
  public void clean()
      throws IOException {
    Preconditions.checkArgument(this.state.contains(ComplianceConfigurationKeys.RETENTION_VERSION_FINDER_CLASS_KEY),
        "Missing required property " + ComplianceConfigurationKeys.RETENTION_VERSION_FINDER_CLASS_KEY);
    Preconditions.checkArgument(this.state.contains(ComplianceConfigurationKeys.RETENTION_SELECTION_POLICY_CLASS_KEY),
        "Missing required property " + ComplianceConfigurationKeys.RETENTION_SELECTION_POLICY_CLASS_KEY);
    Preconditions.checkArgument(this.state.contains(ComplianceConfigurationKeys.RETENTION_VERSION_CLEANER_CLASS_KEY),
        "Missing required property " + ComplianceConfigurationKeys.RETENTION_VERSION_CLEANER_CLASS_KEY);

    List<String> patterns = new ArrayList<>();
    patterns.add(getCompleteTableName(this) + ComplianceConfigurationKeys.BACKUP);
    patterns.add(getCompleteTableName(this) + ComplianceConfigurationKeys.STAGING);
    patterns.add(getCompleteTableName(this) + ComplianceConfigurationKeys.TRASH);

    HivePartitionVersionFinder versionFinder = GobblinConstructorUtils
        .invokeConstructor(HivePartitionVersionFinder.class,
            this.state.getProp(ComplianceConfigurationKeys.RETENTION_VERSION_FINDER_CLASS_KEY), this.fs, this.state,
            patterns);

    List<HivePartitionVersion> versions = new ArrayList<>(versionFinder.findDatasetVersions(this));
    HivePartitionVersionPolicy versionPolicy = GobblinConstructorUtils
        .invokeConstructor(HivePartitionVersionPolicy.class,
            this.state.getProp(ComplianceConfigurationKeys.RETENTION_SELECTION_POLICY_CLASS_KEY), this.state, this);

    List<HivePartitionVersion> deletableVersions = new ArrayList<>(versionPolicy.selectedList(versions));
    List<String> nonDeletableVersionLocations = getNonDeletableVersionLocations(versions, deletableVersions);

    for (HivePartitionVersion hivePartitionDatasetVersion : deletableVersions) {
      try {
        VersionCleaner versionCleaner = GobblinConstructorUtils
            .invokeConstructor(HivePartitionVersionRetentionRunner.class,
                this.state.getProp(ComplianceConfigurationKeys.RETENTION_VERSION_CLEANER_CLASS_KEY), this,
                hivePartitionDatasetVersion, nonDeletableVersionLocations, this.state);
        versionCleaner.clean();
      } catch (Exception e) {
        log.warn("Caught exception trying to clean version " + hivePartitionDatasetVersion.datasetURN() + "\n" + e
            .getMessage());
      }
    }
  }

  private List<String> getNonDeletableVersionLocations(List<HivePartitionVersion> versions,
      List<HivePartitionVersion> deletableVersions) {
    List<String> nonDeletableVersionLocations = new ArrayList<>();
    for (HivePartitionVersion version : versions) {
      if (!deletableVersions.contains(version)) {
        nonDeletableVersionLocations.add(version.getLocation().toString());
      }
    }
    nonDeletableVersionLocations.add(this.getLocation().toString());
    return nonDeletableVersionLocations;
  }

  public String getCompleteTableName(HivePartitionDataset dataset) {
    return StringUtils
        .join(Arrays.asList(dataset.getDbName(), dataset.getTableName()), ComplianceConfigurationKeys.DBNAME_SEPARATOR);
  }
}
