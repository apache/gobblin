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

import org.apache.hadoop.hive.ql.metadata.Partition;

import org.apache.gobblin.data.management.version.DatasetVersion;


/**
 * A class to represent version of a {@link HivePartitionDataset}
 *
 * @author adsharma
 */
public abstract class HivePartitionVersion extends HivePartitionDataset implements DatasetVersion, Comparable<HivePartitionVersion> {

  public HivePartitionVersion(Partition partition) {
    super(partition);
  }

  public HivePartitionVersion(HivePartitionDataset hivePartitionDataset) {
    super(hivePartitionDataset);
  }

  @Override
  public String getVersion() {
    return datasetURN();
  }

  @Override
  public abstract int compareTo(HivePartitionVersion hivePartitionDatasetVersion);
}
