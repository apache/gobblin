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
package gobblin.data.management.conversion.hive.entities;

import lombok.Getter;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.metadata.Partition;


/**
 * A light weight hive partition that is used to serialize into the workunit by the source. This class is used for serialization in favor of the complete {@link Partition}
 * to limit the size of workunit.
 */
public class SerializableHivePartition extends SerializableHiveTable {

  public SerializableHivePartition(String dbName, String tableName, String partitionName, Path schemaUrl) {
    super(dbName, tableName, schemaUrl);
    this.partitionName = partitionName;
  }

  @Getter
  private final String partitionName;

}
