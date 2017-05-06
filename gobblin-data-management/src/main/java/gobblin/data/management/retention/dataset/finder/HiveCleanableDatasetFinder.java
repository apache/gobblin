/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.data.management.retention.dataset.finder;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.metastore.api.Table;

import gobblin.data.management.copy.hive.HiveDataset;
import gobblin.data.management.copy.hive.HiveDatasetFinder;
import gobblin.data.management.retention.dataset.HiveCleanableDataset;


/**
 * Extends {@link HiveDatasetFinder} and finds {@link HiveCleanableDatasetFinder}.
 */
public class HiveCleanableDatasetFinder extends HiveDatasetFinder {
  public HiveCleanableDatasetFinder(FileSystem fs, Properties properties)
      throws IOException {
    super(fs, properties);
  }

  @Override
  protected HiveDataset createHiveDataset(Table table)
      throws IOException {
    return new HiveCleanableDataset(this.fs, this.clientPool, new org.apache.hadoop.hive.ql.metadata.Table(table),
        this.properties);
  }
}
