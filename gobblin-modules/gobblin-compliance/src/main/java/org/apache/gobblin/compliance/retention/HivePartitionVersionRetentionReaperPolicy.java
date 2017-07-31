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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;

import org.apache.gobblin.compliance.ComplianceConfigurationKeys;
import org.apache.gobblin.compliance.HivePartitionDataset;
import org.apache.gobblin.compliance.HivePartitionVersion;
import org.apache.gobblin.compliance.HivePartitionVersionPolicy;
import org.apache.gobblin.configuration.State;


public class HivePartitionVersionRetentionReaperPolicy extends HivePartitionVersionPolicy {
  /**
   * Number of days after which a {@link HivePartitionVersion} will be either moved to
   * backup or deleted.
   */
  private int retentionDays;

  public HivePartitionVersionRetentionReaperPolicy(State state, HivePartitionDataset dataset) {
    super(state, dataset);
    Preconditions.checkArgument(state.contains(ComplianceConfigurationKeys.REAPER_RETENTION_DAYS),
        "Missing required property " + ComplianceConfigurationKeys.REAPER_RETENTION_DAYS);
    this.retentionDays = state.getPropAsInt(ComplianceConfigurationKeys.REAPER_RETENTION_DAYS);
  }

  @Override
  public boolean shouldSelect(HivePartitionVersion version) {
    HivePartitionRetentionVersion partitionVersion = (HivePartitionRetentionVersion) version;
    long ageInDays = TimeUnit.MILLISECONDS.toDays(partitionVersion.getAgeInMilliSeconds());
    return ageInDays >= this.retentionDays;
  }

  @Override
  public List<HivePartitionVersion> selectedList(List<HivePartitionVersion> versions) {
    if (versions.isEmpty()) {
      return versions;
    }

    Preconditions.checkArgument(versions.get(0) instanceof HivePartitionRetentionVersion);
    List<HivePartitionVersion> selectedVersions = new ArrayList<>();
    Collections.sort(versions);
    for (HivePartitionVersion version : versions) {
      if (shouldSelect(version)) {
        selectedVersions.add(version);
      }
    }
    return selectedVersions;
  }
}
