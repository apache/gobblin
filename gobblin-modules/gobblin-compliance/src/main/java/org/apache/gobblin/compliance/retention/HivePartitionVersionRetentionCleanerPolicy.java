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

import com.google.common.base.Preconditions;
import org.apache.gobblin.compliance.ComplianceConfigurationKeys;
import org.apache.gobblin.compliance.HivePartitionDataset;
import org.apache.gobblin.compliance.HivePartitionVersion;
import org.apache.gobblin.compliance.HivePartitionVersionPolicy;
import org.apache.gobblin.configuration.State;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;


/**
 * A retention version policy for the {@link HivePartitionRetentionVersion}.
 *
 * @author adsharma
 */
@Slf4j
public class HivePartitionVersionRetentionCleanerPolicy extends HivePartitionVersionPolicy {
  /**
   * Maximum number of backups to be retained.
   */
  private int backupRetentionVersions;
  /**
   * Maximum age for a retained backup.
   */
  private int backupRetentionDays;
  /**
   * Maximum age for a retained trash partition.
   */
  private int trashRetentionDays;

  public HivePartitionVersionRetentionCleanerPolicy(State state, HivePartitionDataset dataset) {
    super(state, dataset);
    Preconditions.checkArgument(this.state.contains(ComplianceConfigurationKeys.CLEANER_BACKUP_RETENTION_VERSIONS),
        "Missing required property " + ComplianceConfigurationKeys.CLEANER_BACKUP_RETENTION_VERSIONS);
    Preconditions.checkArgument(this.state.contains(ComplianceConfigurationKeys.CLEANER_BACKUP_RETENTION_DAYS),
        "Missing required property " + ComplianceConfigurationKeys.CLEANER_BACKUP_RETENTION_DAYS);
    Preconditions.checkArgument(this.state.contains(ComplianceConfigurationKeys.CLEANER_TRASH_RETENTION_DAYS),
        "Missing required property " + ComplianceConfigurationKeys.CLEANER_TRASH_RETENTION_DAYS);
    this.backupRetentionVersions =
        this.state.getPropAsInt(ComplianceConfigurationKeys.CLEANER_BACKUP_RETENTION_VERSIONS);
    this.backupRetentionDays = this.state.getPropAsInt(ComplianceConfigurationKeys.CLEANER_BACKUP_RETENTION_DAYS);
    this.trashRetentionDays = this.state.getPropAsInt(ComplianceConfigurationKeys.CLEANER_TRASH_RETENTION_DAYS);
  }

  @Override
  public boolean shouldSelect(HivePartitionVersion version) {
    // not implemented by this class
    return false;
  }

  @Override
  public List<HivePartitionVersion> selectedList(List<HivePartitionVersion> versions) {
    if (versions.isEmpty()) {
      return versions;
    }
    List<HivePartitionRetentionVersion> backupVersions = new ArrayList<>();
    List<HivePartitionRetentionVersion> trashVersions = new ArrayList<>();
    List<HivePartitionVersion> selectedVersions = new ArrayList<>();
    for (HivePartitionVersion version : versions) {
      String prefix = this.dataset.getDbName() + ComplianceConfigurationKeys.DBNAME_SEPARATOR;
      if (!version.getTableName().startsWith(prefix)) {
        continue;
      }
      if (version.getTableName().contains(ComplianceConfigurationKeys.BACKUP)) {
        backupVersions.add((HivePartitionRetentionVersion) version);
      }
      if (version.getTableName().contains(ComplianceConfigurationKeys.TRASH)) {
        trashVersions.add((HivePartitionRetentionVersion) version);
      }
    }

    for (HivePartitionRetentionVersion version : trashVersions) {
      long ageInDays = TimeUnit.MILLISECONDS.toDays(version.getAgeInMilliSeconds());
      if (ageInDays >= this.trashRetentionDays) {
        selectedVersions.add(version);
      }
    }

    if (backupVersions.isEmpty()) {
      return selectedVersions;
    }
    Collections.sort(backupVersions);
    selectedVersions.addAll(backupVersions.subList(this.backupRetentionVersions, versions.size()));
    if (this.backupRetentionVersions == 0) {
      return selectedVersions;
    }

    for (HivePartitionRetentionVersion version : backupVersions.subList(0, this.backupRetentionVersions)) {
      long ageInDays = TimeUnit.MILLISECONDS.toDays(version.getAgeInMilliSeconds());
      if (ageInDays >= this.backupRetentionDays) {
        selectedVersions.add(version);
      }
    }
    return selectedVersions;
  }
}
