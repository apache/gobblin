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

import lombok.AllArgsConstructor;
import lombok.Getter;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.metadata.Table;
/**
 * A light weight hive table that is used to serialize into the workunit by the source. This class is used for serialization in favor of the complete {@link Table}
 * to limit the size of workunit.
 */
@AllArgsConstructor
@Getter
public class SerializableHiveTable {

  private final String dbName;
  private final String tableName;
  private final Path schemaUrl;
}
