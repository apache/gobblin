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

import org.apache.iceberg.catalog.Catalog;


/**
 * Any catalog from which to access {@link IcebergTable}s.
 */
public interface IcebergCatalog {
  IcebergTable openTable(String dbName, String tableName);
  String getCatalogUri();

  /**
   * Adding a sub interface to help us provide an association between {@link Catalog} and {@link IcebergCatalog}.
   * This helps us resolve to the Catalog to its concrete implementation class
   * Primarily needed to access `newTableOps` method which only certain {@link Catalog} derived classes open for public access
   */
  interface CatalogSpecifier {
    Class<? extends Catalog> getCatalogClass();
    Class<? extends IcebergCatalog> getIcebergCatalogClass();
    String getCatalogName();
  }

}
