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

import org.apache.avro.Schema;
import org.apache.hadoop.hive.ql.metadata.Table;

/**
 * An extension to the {@link Table} that also knows the {@link Schema} of this {@link Table}
 */
public class SchemaAwareHiveTable extends Table {

  @Getter
  private final Schema avroSchema;
  private static final long serialVersionUID = 1856720117875056735L;

  public SchemaAwareHiveTable(org.apache.hadoop.hive.metastore.api.Table table, Schema schema) {
    super(table);
    this.avroSchema = schema;
  }
}
