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

  // Only set if table has exactly one location
  protected final Optional<Path> tableRootPath;

  protected final String tableIdentifier;


  public HiveDataset(FileSystem fs, HiveMetastoreClientPool clientPool, Table table, Properties properties) throws IOException {
    this.fs = fs;
    this.clientPool = clientPool;
    this.table = table;
    this.properties = properties;

    this.tableRootPath = PathUtils.isGlob(this.table.getDataLocation()) ? Optional.<Path>absent() :
        Optional.of(this.table.getDataLocation());

    this.tableIdentifier = this.table.getDbName() + "." + this.table.getTableName();
    log.info("Created Hive dataset for table " + tableIdentifier);
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

  @Override public String datasetURN() {
    return this.table.getCompleteName();
  }

}
