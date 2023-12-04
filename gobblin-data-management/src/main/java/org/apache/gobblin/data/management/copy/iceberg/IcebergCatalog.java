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

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.TableIdentifier;


/**
 * Any catalog from which to access {@link IcebergTable}s.
 */
public interface IcebergCatalog {

  /** @return table identified by `dbName` and `tableName` */
  IcebergTable openTable(String dbName, String tableName);

  /** @return table identified by `tableId` */
  default IcebergTable openTable(TableIdentifier tableId) {
    // CHALLENGE: clearly better to implement in the reverse direction - `openTable(String, String)` in terms of `openTable(TableIdentifier)` -
    // but challenging to do at this point, with multiple derived classes already "in the wild" that implement `openTable(String, String)`
    return openTable(tableId.namespace().toString(), tableId.name());
  }

  String getCatalogUri();

  void initialize(Map<String, String> properties, Configuration configuration);

  boolean tableAlreadyExists(IcebergTable icebergTable);
}
