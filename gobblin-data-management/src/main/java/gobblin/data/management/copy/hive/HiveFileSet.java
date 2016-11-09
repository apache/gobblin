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

package gobblin.data.management.copy.hive;

import org.apache.hadoop.hive.ql.metadata.Table;

import gobblin.data.management.copy.CopyEntity;
import gobblin.data.management.partition.FileSet;

import lombok.Getter;


/**
 * A {@link FileSet} for Hive datasets. Contains information on Hive table.
 */
@Getter
public abstract class HiveFileSet extends FileSet<CopyEntity> {

  private final Table table;
  private final HiveDataset hiveDataset;

  public HiveFileSet(String name, HiveDataset dataset) {
    super(name, dataset);
    this.table = dataset.getTable();
    this.hiveDataset = dataset;
  }
}
