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
package gobblin.compliance.utils;

import java.util.List;

import com.google.common.base.Optional;

import lombok.extern.slf4j.Slf4j;

import gobblin.compliance.HivePartitionDataset;


/**
 * @author adsharma
 */
@Slf4j
public class DatasetUtils {
  /**
   * Find {@link HivePartitionDataset} given complete partition name from a list of datasets
   * @param partitionName Complete partition name ie dbName@tableName@partitionName
   */
  public static Optional<HivePartitionDataset> findDataset(String partitionName, List<HivePartitionDataset> datasets) {
    for (HivePartitionDataset dataset : datasets) {
      if (dataset.datasetURN().equalsIgnoreCase(partitionName)) {
        return Optional.fromNullable(dataset);
      }
    }
    log.warn("Unable to find dataset corresponding to " + partitionName);
    return Optional.<HivePartitionDataset>absent();
  }

  public static String getProperty(HivePartitionDataset dataset, String property, long defaultValue) {
    Optional<String> propertyValueOptional = Optional.fromNullable(dataset.getParams().get(property));
    if (!propertyValueOptional.isPresent()) {
      return Long.toString(defaultValue);
    }
    try {
      long propertyVal = Long.parseLong(propertyValueOptional.get());
      if (propertyVal < 0) {
        return Long.toString(defaultValue);
      } else {
        return Long.toString(propertyVal);
      }
    } catch (NumberFormatException e) {
      return Long.toString(defaultValue);
    }
  }
}
