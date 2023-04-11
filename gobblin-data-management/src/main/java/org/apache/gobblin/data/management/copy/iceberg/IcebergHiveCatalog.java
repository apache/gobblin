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
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hive.HiveCatalog;

import lombok.extern.slf4j.Slf4j;


/**
 * Hive-Metastore-based {@link IcebergCatalog}.
 */
@Slf4j

public class IcebergHiveCatalog extends BaseIcebergCatalog {
  public static final String HIVE_CATALOG_NAME = "HiveCatalog";
  // NOTE: specifically necessitates `HiveCatalog`, as `BaseMetastoreCatalog.newTableOps` is `protected`!
  private HiveCatalog hc;

  public IcebergHiveCatalog() {
    super(HIVE_CATALOG_NAME, HiveCatalog.class);
  }

  @Override
  public void initialize(Map<String, String> properties, Configuration configuration) {
    hc = (HiveCatalog) createCompanionCatalog(properties, configuration);
  }

  @Override
  public String getCatalogUri() {
    return hc.getConf().get(HiveConf.ConfVars.METASTOREURIS.varname, "<<not set>>");
  }

  @Override
  protected TableOperations createTableOperations(TableIdentifier tableId) {
    return hc.newTableOps(tableId);
  }

  @Override
  public boolean tableAlreadyExists(IcebergTable icebergTable) {
    return hc.tableExists(icebergTable.getTableId());
  }
}
