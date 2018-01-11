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
package org.apache.gobblin.data.management.version.finder;

import java.io.IOException;

import org.apache.gobblin.data.management.version.TimestampedHiveDatasetVersion;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.joda.time.DateTime;

import com.typesafe.config.Config;

import parquet.Preconditions;


/**
 * A Hive Partition finder where the the version is the partition value.
 */
public class HdfsModifiedTimeHiveVersionFinder extends AbstractHiveDatasetVersionFinder {
  private final FileSystem fs;

  public HdfsModifiedTimeHiveVersionFinder(FileSystem fs, Config config) {
    this.fs = fs;
  }

  /**
   * Create a {@link TimestampedHiveDatasetVersion} from a {@link Partition} based on the Modified time of underlying
   * hdfs data location
   * @throws IllegalArgumentException when argument is null
   * @throws IllegalArgumentException when data location of partition is null
   * @throws IllegalArgumentException when data location of partition doesn't exist
   * {@inheritDoc}
   */
  @Override
  protected TimestampedHiveDatasetVersion getDatasetVersion(Partition partition) {
    try {
      Preconditions.checkArgument(partition != null, "Argument to method ");

      Path dataLocation = partition.getDataLocation();
      Preconditions
          .checkArgument(dataLocation != null, "Data location is null for partition " + partition.getCompleteName());
      boolean exists = this.fs.exists(dataLocation);
      Preconditions.checkArgument(exists, "Data location doesn't exist for partition " + partition.getCompleteName());

      long modificationTS = this.fs.getFileStatus(dataLocation).getModificationTime();
      return new TimestampedHiveDatasetVersion(new DateTime(modificationTS), partition);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
