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
package gobblin.data.management.conversion.hive.entities;

import lombok.EqualsAndHashCode;
import lombok.Getter;

import org.apache.avro.Schema;
import org.apache.hadoop.hive.ql.metadata.Table;

/**
 * An extension to the {@link Table} that also knows the {@link Schema} of this {@link Table}
 */
@EqualsAndHashCode(callSuper=true)
public class SchemaAwareHiveTable extends Table {

  @Getter
  private final Schema avroSchema;
  private static final long serialVersionUID = 1856720117875056735L;

  public SchemaAwareHiveTable(org.apache.hadoop.hive.metastore.api.Table table, Schema schema) {
    super(table);
    this.avroSchema = schema;
  }
}
