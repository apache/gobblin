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
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;

/**
 * Base implementation of {@link IcebergCatalog} to access {@link IcebergTable} and the
 * underlying concrete companion catalog e.g. {@link org.apache.iceberg.hive.HiveCatalog}
 */
public abstract class BaseIcebergCatalog implements IcebergCatalog {

  protected final String catalogName;
  protected final Class<? extends Catalog> companionCatalogClass;

  protected BaseIcebergCatalog(String catalogName, Class<? extends Catalog> companionCatalogClass) {
    this.catalogName = catalogName;
    this.companionCatalogClass = companionCatalogClass;
  }

  @Override
  public IcebergTable openTable(String dbName, String tableName) {
    TableIdentifier tableId = TableIdentifier.of(dbName, tableName);
    return new IcebergTable(tableId, createTableOperations(tableId), this.getCatalogUri());
  }

  protected Catalog createCompanionCatalog(Map<String, String> properties, Configuration configuration) {
    return CatalogUtil.loadCatalog(this.companionCatalogClass.getName(), this.catalogName, properties, configuration);
  }

  protected abstract TableOperations createTableOperations(TableIdentifier tableId);
}
