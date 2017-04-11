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
package gobblin.data.management.conversion.hive.provider;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import lombok.AllArgsConstructor;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import gobblin.hive.HivePartition;
import gobblin.hive.HiveTable;


/**
 * Uses the file modification time of the data location of a {@link HiveTable} or {@link HivePartition} on HDFS
 */
@AllArgsConstructor
public class HdfsBasedUpdateProvider implements HiveUnitUpdateProvider {

  private final FileSystem fs;

  // Cache modification times of data location to reduce the number of HDFS calls
  private static final Cache<Path, Long> PATH_TO_MOD_TIME_CACHE = CacheBuilder.newBuilder().maximumSize(2000).build();

  /**
   * Get the update time of a {@link Partition}
   *
   * @return the update time if available, 0 otherwise
   *
   * {@inheritDoc}
   * @see HiveUnitUpdateProvider#getUpdateTime(org.apache.hadoop.hive.ql.metadata.Partition)
   */
  @Override
  public long getUpdateTime(Partition partition) throws UpdateNotFoundException {

    try {
      return getUpdateTime(partition.getDataLocation());
    } catch (IOException e) {
      throw new UpdateNotFoundException(String.format("Failed to get update time for %s", partition.getCompleteName()),
          e);
    }
  }

  /**
   * Get the update time of a {@link Table}
   * @return the update time if available, 0 otherwise
   *
   * {@inheritDoc}
   * @see HiveUnitUpdateProvider#getUpdateTime(org.apache.hadoop.hive.ql.metadata.Table)
   */
  @Override
  public long getUpdateTime(Table table) throws UpdateNotFoundException {
    try {
      return getUpdateTime(table.getDataLocation());
    } catch (IOException e) {
      throw new UpdateNotFoundException(String.format("Failed to get update time for %s.", table.getCompleteName()), e);
    }
  }

  private long getUpdateTime(final Path path) throws IOException, UpdateNotFoundException {

    try {
      return PATH_TO_MOD_TIME_CACHE.get(path, new Callable<Long>() {
        @Override
        public Long call() throws Exception {
          if (HdfsBasedUpdateProvider.this.fs.exists(path)) {
            return HdfsBasedUpdateProvider.this.fs.getFileStatus(path).getModificationTime();
          }
          throw new UpdateNotFoundException(String.format("Data file does not exist at path %s", path));
        }
      });
    } catch (ExecutionException e) {
      throw new IOException(e);
    }
  }
}
