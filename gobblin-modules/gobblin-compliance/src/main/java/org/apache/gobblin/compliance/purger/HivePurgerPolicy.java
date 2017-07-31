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
package org.apache.gobblin.compliance.purger;

import com.google.common.base.Preconditions;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.compliance.ComplianceConfigurationKeys;
import org.apache.gobblin.compliance.HivePartitionDataset;


/**
 * A policy class to determine if a dataset should be purged or not.
 */
@Slf4j
public class HivePurgerPolicy implements PurgePolicy<HivePartitionDataset> {
  private String watermark;

  public HivePurgerPolicy(String watermark) {
    Preconditions.checkNotNull(watermark, "Watermark should not be null");
    this.watermark = watermark;
  }

  public boolean shouldPurge(HivePartitionDataset dataset) {
    if (!dataset.getTableParams().containsKey(ComplianceConfigurationKeys.DATASET_DESCRIPTOR_KEY)) {
      return false;
    }
    if (this.watermark.equalsIgnoreCase(ComplianceConfigurationKeys.NO_PREVIOUS_WATERMARK)) {
      return true;
    }
    return dataset.datasetURN().compareTo(this.watermark) >= 0;
  }
}
