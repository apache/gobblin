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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.commons.lang3.reflect.ConstructorUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;

import org.apache.thrift.TException;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;


/**
 * Utilities for {@link org.apache.hadoop.hive.ql} classes.
 */
public class HiveUtils {

  /**
   * @param client an {@link IMetaStoreClient} for the correct metastore.
   * @param table the {@link Table} for which we should get partitions.
   * @param filter an optional filter for partitions as would be used in Hive. Can only filter on String columns.
   *               (e.g. "part = \"part1\"" or "date > \"2015\"".
   * @return a map of values to {@link Partition} for input {@link Table}.
   */
  public static Map<List<String>, Partition> getPartitionsMap(IMetaStoreClient client, Table table,
      Optional<String> filter, Optional<? extends HivePartitionExtendedFilter> hivePartitionExtendedFilterOptional) throws IOException {
    return Maps.uniqueIndex(getPartitions(client, table, filter, hivePartitionExtendedFilterOptional), new Function<Partition, List<String>>() {
      @Override
      public List<String> apply(@Nullable Partition partition) {
        if (partition == null) {
          return null;
        }
        return partition.getValues();
      }
    });
  }

  /**
   * Get a list of {@link Partition}s for the <code>table</code> that matches an optional <code>filter</code>
   *
   * @param client an {@link IMetaStoreClient} for the correct metastore.
   * @param table the {@link Table} for which we should get partitions.
   * @param filter an optional filter for partitions as would be used in Hive. Can only filter on String columns.
   *               (e.g. "part = \"part1\"" or "date > \"2015\"".
   * @return a list of {@link Partition}s
   */
  public static List<Partition> getPartitions(IMetaStoreClient client, Table table,
      Optional<String> filter, Optional<? extends HivePartitionExtendedFilter> hivePartitionExtendedFilterOptional)
      throws IOException {
    try {
      List<Partition> partitions = Lists.newArrayList();
      List<org.apache.hadoop.hive.metastore.api.Partition> partitionsList = filter.isPresent()
          ? client.listPartitionsByFilter(table.getDbName(), table.getTableName(), filter.get(), (short) -1)
          : client.listPartitions(table.getDbName(), table.getTableName(), (short) -1);
      for (org.apache.hadoop.hive.metastore.api.Partition p : partitionsList) {
        if (!hivePartitionExtendedFilterOptional.isPresent() ||
            hivePartitionExtendedFilterOptional.get().accept(p)) {
          Partition partition = new Partition(table, p);
          partitions.add(partition);
        }
      }
      return partitions;
    } catch (TException | HiveException te) {
      throw new IOException("Hive Error", te);
    }
  }

  /**
   * For backward compatibility when PathFilter is injected as a parameter.
   * @param client
   * @param table
   * @param filter
   * @return
   * @throws IOException
   */
  public static List<Partition> getPartitions(IMetaStoreClient client, Table table, Optional<String> filter)
      throws IOException {
    return getPartitions(client, table, filter, Optional.<HivePartitionExtendedFilter>absent());
  }

  /**
   * @return an instance of the {@link InputFormat} in this {@link StorageDescriptor}.
   */
  public static InputFormat<?, ?> getInputFormat(StorageDescriptor sd) throws IOException {
    try {
      InputFormat<?, ?> inputFormat =
          ConstructorUtils.invokeConstructor((Class<? extends InputFormat>) Class.forName(sd.getInputFormat()));
      if (inputFormat instanceof JobConfigurable) {
        ((JobConfigurable) inputFormat).configure(new JobConf(getHadoopConfiguration()));
      }
      return inputFormat;
    } catch (ReflectiveOperationException re) {
      throw new IOException("Failed to instantiate input format.", re);
    }
  }

  /**
   * Get paths from a Hive location using the provided input format.
   */
  public static Set<Path> getPaths(InputFormat<?, ?> inputFormat, Path location) throws IOException {
    JobConf jobConf = new JobConf(getHadoopConfiguration());

    Set<Path> paths = Sets.newHashSet();

    FileInputFormat.addInputPaths(jobConf, location.toString());
    InputSplit[] splits = inputFormat.getSplits(jobConf, 1000);
    for (InputSplit split : splits) {
      if (!(split instanceof FileSplit)) {
        throw new IOException("Not a file split. Found " + split.getClass().getName());
      }
      FileSplit fileSplit = (FileSplit) split;
      paths.add(fileSplit.getPath());
    }

    return paths;
  }

  private static Configuration getHadoopConfiguration() {
    Configuration conf = new Configuration();
    if (System.getenv("HADOOP_TOKEN_FILE_LOCATION") != null) {
      conf.set("mapreduce.job.credentials.binary", System.getenv("HADOOP_TOKEN_FILE_LOCATION"));
    }
    return conf;
  }

  /**
   * @return true if {@link Table} is partitioned.
   */
  public static boolean isPartitioned(Table table) {
    return table.isPartitioned();
  }
}
