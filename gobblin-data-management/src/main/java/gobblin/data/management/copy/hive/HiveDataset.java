/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.data.management.copy.hive;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang3.reflect.ConstructorUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.thrift.TException;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import gobblin.annotation.Alpha;
import gobblin.data.management.copy.CopyConfiguration;
import gobblin.data.management.copy.CopyEntity;
import gobblin.data.management.copy.CopyableDataset;
import gobblin.hive.HiveMetastoreClientPool;
import gobblin.util.AutoReturnableObject;
import gobblin.util.PathUtils;


/**
 * Hive dataset implementing {@link CopyableDataset}.
 */
@Slf4j
@Alpha
public class HiveDataset implements CopyableDataset {

  protected final Properties properties;
  protected final FileSystem fs;
  protected final HiveMetastoreClientPool clientPool;
  protected final Table table;
  protected static final Splitter splitter = Splitter.on(",").omitEmptyStrings();
  protected static final Joiner joiner = Joiner.on(",").skipNulls();

  protected final List<Path> tableLocations;
  // Only set if table has exactly one location
  protected final Optional<Path> tableRootPath;

  protected final String tableIdentifier;


  public HiveDataset(FileSystem fs, HiveMetastoreClientPool clientPool, Table table, Properties properties) throws IOException {
    this.fs = fs;
    this.clientPool = clientPool;
    this.table = table;
    this.properties = properties;

    this.tableLocations = parseLocationIntoPaths(this.table.getDataLocation());

    this.tableRootPath = this.tableLocations.size() == 1 ?
        Optional.of(PathUtils.deepestNonGlobPath(this.tableLocations.get(0))) : Optional.<Path>absent();

    this.tableIdentifier = this.table.getDbName() + "." + this.table.getTableName();
    log.info("Created Hive dataset for table " + tableIdentifier);
  }

  /**
   * Parse a location string from a {@link StorageDescriptor} into an actual list of globs.
   */
  protected static List<Path> parseLocationIntoPaths(Path sdLocation) {
    List<Path> locations = Lists.newArrayList();

    if (sdLocation == null) {
      return locations;
    }

    for (String location : splitter.split(sdLocation.toString())) {
      locations.add(new Path(location));
    }
    return locations;
  }

  /**
   * Finds all files read by the table and generates CopyableFiles.
   * For the specific semantics see {@link HiveCopyEntityHelper#getCopyEntities}.
   */
  @Override public Collection<CopyEntity> getCopyableFiles(FileSystem targetFs, CopyConfiguration configuration)
      throws IOException {
    try {
      return new HiveCopyEntityHelper(this, configuration, targetFs).getCopyEntities();
    } catch (IOException ioe) {
      log.error("Failed to copy table " + this.table, ioe);
      return Lists.newArrayList();
    }
  }

  protected static Map<List<String>, Partition> getPartitions(AutoReturnableObject<IMetaStoreClient> client, Table table)
      throws IOException {
    try {
      Map<List<String>, Partition> partitions = Maps.newHashMap();
      for (org.apache.hadoop.hive.metastore.api.Partition p : client.get()
          .listPartitions(table.getDbName(), table.getTableName(), (short) -1)) {
        Partition partition = new Partition(table, p);
        partitions.put(partition.getValues(), partition);
      }
      return partitions;
    } catch (TException | HiveException te) {
      throw new IOException("Hive Error", te);
    }
  }

  protected static InputFormat<?, ?> getInputFormat(StorageDescriptor sd) throws IOException {
    try {
      InputFormat<?, ?> inputFormat = ConstructorUtils.invokeConstructor(
          (Class<? extends InputFormat>) Class.forName(sd.getInputFormat()));
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
  protected static Collection<Path> getPaths(InputFormat<?, ?> inputFormat, String location) throws IOException {
    JobConf jobConf = new JobConf(getHadoopConfiguration());

    Set<Path> paths = Sets.newHashSet();

    FileInputFormat.addInputPaths(jobConf, location);
    InputSplit[] splits = inputFormat.getSplits(jobConf, 1000);
    for (InputSplit split : splits) {
      if (!(split instanceof FileSplit)) {
        throw new IOException("Not a file split.");
      }
      FileSplit fileSplit = (FileSplit) split;
      paths.add(fileSplit.getPath());
    }

    return paths;
  }

  protected static Configuration getHadoopConfiguration() {
    Configuration conf = new Configuration();
    if (System.getenv("HADOOP_TOKEN_FILE_LOCATION") != null) {
      conf.set("mapreduce.job.credentials.binary", System.getenv("HADOOP_TOKEN_FILE_LOCATION"));
    }
    return conf;
  }

  public static boolean isPartitioned(Table table) {
    return table.isPartitioned();
  }

  @Override public String datasetURN() {
    return this.table.getDbName() + "." + this.table.getTableName();
  }

}
