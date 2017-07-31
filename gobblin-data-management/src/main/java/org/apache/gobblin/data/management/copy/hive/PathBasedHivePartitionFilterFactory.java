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


package org.apache.gobblin.data.management.copy.hive;

import java.util.Properties;

import com.typesafe.config.Config;

import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.annotation.Alias;

/**
 * A path based specific filter factory for generation of {@link PathBasedPartitionFilter}
 */
@Alias("PathPartition")
public class PathBasedHivePartitionFilterFactory implements HivePartitionExtendedFilterFactory {
  /* Regular expression components required for filtering partitions by their path */
  public static final String HIVE_PARTITION_PATH_FILTER_REGEX = HiveDatasetFinder.HIVE_DATASET_PREFIX + ".pathFilterRegex";

  @Override
  public HivePartitionExtendedFilter createFilter(Config config){
    Properties props = ConfigUtils.configToProperties(config);
    return props.containsKey(PathBasedHivePartitionFilterFactory.HIVE_PARTITION_PATH_FILTER_REGEX)?
        new PathBasedPartitionFilter(props.getProperty(PathBasedHivePartitionFilterFactory.HIVE_PARTITION_PATH_FILTER_REGEX))
        :null;
  }
}
