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

package gobblin.data.management.copy.hive;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.metastore.api.Partition;


/**
 * One simple implementation for {@link HivePartitionExtendedFilter},
 * which looked at each partition's path and decide if a partition should be kept or not.
 */
public class PathBasedPartitionFilter implements HivePartitionExtendedFilter {

  /**
   * A pair of configs indicating the type of extended filter and the filtering policy.
   * Note that only when type is specified will the policy be taken into effect.
   *
   * For example, if you specify "Path" as the filter type and "Hourly" as the policy,
   * partitions with Path containing '/Hourly/' will be kept.
   */
  public static final String HIVE_PARTITION_EXTENDED_FILTER_TYPE = HiveDatasetFinder.HIVE_DATASET_PREFIX + ".extended.filter.type";
  public static final String HIVE_PARTITION_PATH_FILTER_POLICY = HiveDatasetFinder.HIVE_DATASET_PREFIX + ".path.filter.policy";

  private String filterPolicy;

  public PathBasedPartitionFilter(String filterPolicy) {
    this.filterPolicy = filterPolicy;
  }

  @Override
  public boolean accept(Partition partition){
    PathFilter pathFilter = new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return path.toString().contains("/" + filterPolicy +"/");
      }
    };
    return pathFilter.accept(new Path(partition.getSd().getLocation()));
  }
}
