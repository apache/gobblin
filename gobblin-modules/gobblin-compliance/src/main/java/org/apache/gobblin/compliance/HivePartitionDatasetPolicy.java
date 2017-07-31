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
package org.apache.gobblin.compliance;

import java.util.ArrayList;
import java.util.List;


/**
 * This policy checks if a Hive Partition is a valid dataset.
 * Hive table to which Hive partition belongs must be an external table.
 * Hive table name must not contain "_trash_", "_staging_" and "_backup_"
 * in it's name since they will considered as {@link HivePartitionVersion} rather
 * than {@link HivePartitionDataset}
 */
public class HivePartitionDatasetPolicy implements Policy<HivePartitionDataset> {
  @Override
  public boolean shouldSelect(HivePartitionDataset dataset) {
    if (dataset.getTableName().contains(ComplianceConfigurationKeys.TRASH)) {
      return false;
    } else if (dataset.getTableName().contains(ComplianceConfigurationKeys.BACKUP)) {
      return false;
    } else if (dataset.getTableName().contains(ComplianceConfigurationKeys.STAGING)) {
      return false;
    } else {
      return dataset.getTableMetadata().containsKey(ComplianceConfigurationKeys.EXTERNAL);
    }
  }

  @Override
  public List<HivePartitionDataset> selectedList(List<HivePartitionDataset> datasets) {
    List<HivePartitionDataset> selectedDatasetList = new ArrayList<>();
    for (HivePartitionDataset dataset : datasets) {
      if (shouldSelect(dataset)) {
        selectedDatasetList.add(dataset);
      }
    }
    return selectedDatasetList;
  }
}
