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

package org.apache.gobblin.data.management.copy.iceberg;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hive.HiveCatalog;


/**
 * Hive-Metastore-based {@link IcebergCatalog}.
 */
@Slf4j
@AllArgsConstructor
public class IcebergHiveCatalog implements IcebergCatalog {
  // NOTE: specifically necessitates `HiveCatalog`, as `BaseMetastoreCatalog.newTableOps` is `protected`!
  private final HiveCatalog hc;

  @Override
  public IcebergTable openTable(String dbName, String tableName) {
    TableIdentifier tableId = TableIdentifier.of(dbName, tableName);
    return new IcebergTable(tableId, hc.newTableOps(tableId));
  }
}
