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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.metastore.api.Partition;

/**
 * One simple implementation for {@link HivePartitionExtendedFilter},
 * which looked at each partition's path and decide if a partition should be kept or not.
 */
public class PathBasedPartitionFilter implements HivePartitionExtendedFilter {

  private String filterRegex;
  private Pattern pattern;

  public PathBasedPartitionFilter(String filterRegex) {
    this.filterRegex = filterRegex;
    pattern = Pattern.compile(filterRegex);
  }

  @Override
  /* For partitions with path that contains filterRegex as part of it, will be filtered out. */
  public boolean accept(Partition partition){
    Matcher matcher = pattern.matcher(partition.getSd().getLocation());
    return matcher.find();
  }
}
