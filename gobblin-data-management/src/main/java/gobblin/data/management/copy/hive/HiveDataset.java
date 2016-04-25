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

import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.metadata.Table;

import com.google.common.base.Optional;
import com.google.common.collect.Iterators;

import gobblin.annotation.Alpha;
import gobblin.data.management.copy.CopyConfiguration;
import gobblin.data.management.copy.CopyEntity;
import gobblin.data.management.copy.CopyableDataset;
import gobblin.data.management.copy.IterableCopyableDataset;
import gobblin.data.management.partition.FileSet;
import gobblin.hive.HiveMetastoreClientPool;
import gobblin.util.PathUtils;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;


/**
 * Hive dataset implementing {@link CopyableDataset}.
 */
@Slf4j
@Alpha
@Getter
public class HiveDataset implements IterableCopyableDataset {

  public static final String REGISTERER = "registerer";
  public static final String REGISTRATION_GENERATION_TIME_MILLIS = "registrationGenerationTimeMillis";

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
  @Override public Iterator<FileSet<CopyEntity>> getFileSetIterator(FileSystem targetFs, CopyConfiguration configuration)
      throws IOException {
    try {
      return new HiveCopyEntityHelper(this, configuration, targetFs).getCopyEntities();
    } catch (IOException ioe) {
      log.error("Failed to copy table " + this.table, ioe);
      return Iterators.emptyIterator();
    }
  }

  @Override public String datasetURN() {
    return this.table.getCompleteName();
  }

}
