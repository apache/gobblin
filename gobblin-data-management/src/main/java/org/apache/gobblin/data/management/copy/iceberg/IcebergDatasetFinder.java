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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.dataset.DatasetConstants;
import org.apache.gobblin.dataset.IterableDatasetFinder;
import org.apache.gobblin.util.HadoopUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Finds {@link IcebergDataset}s. Will look for tables in a database using a {@link IcebergCatalog},
 * and creates a {@link IcebergDataset} for each one.
 */
@Slf4j
@AllArgsConstructor
public class IcebergDatasetFinder implements IterableDatasetFinder<IcebergDataset> {

  public static final String ICEBERG_DATASET_PREFIX = "iceberg.dataset";
  public static final String ICEBERG_METASTORE_URI_KEY = ICEBERG_DATASET_PREFIX + ".hive.metastore.uri";
  public static final String ICEBERG_DB_NAME = DatasetConstants.PLATFORM_ICEBERG + ".database.name";
  public static final String ICEBERG_TABLE_NAME = DatasetConstants.PLATFORM_ICEBERG + ".table.name";

  private String dbName;
  private String tblName;
  private final Properties properties;
  protected final FileSystem fs;

  @Override
  public List<IcebergDataset> findDatasets() throws IOException {
    List<IcebergDataset> matchingDatasets = new ArrayList<>();
    /*
     * Both Iceberg database name and table name are mandatory,
     * since we are currently only supporting Hive Catalog based Iceberg tables.
     * The design will support defaults and other catalogs in future releases.
     */
    if (properties.getProperty(ICEBERG_DB_NAME) == null || properties.getProperty(ICEBERG_TABLE_NAME) == null) {
      throw new IOException("Iceberg database name or Iceberg table name is missing");
    }
    this.dbName = properties.getProperty(ICEBERG_DB_NAME);
    this.tblName = properties.getProperty(ICEBERG_TABLE_NAME);

    Configuration configuration = HadoopUtils.getConfFromProperties(properties);

    IcebergCatalog icebergCatalog = IcebergCatalogFactory.create(configuration);
    IcebergTable icebergTable = icebergCatalog.openTable(dbName, tblName);
    // Currently, we only support one dataset per iceberg table
    matchingDatasets.add(createIcebergDataset(dbName, tblName, icebergTable, properties, fs));
    log.info("Found {} matching datasets: {}", matchingDatasets.size(), matchingDatasets);

    return matchingDatasets;
  }

  @Override
  public Path commonDatasetRoot() {
    return null;
  }

  @Override
  public Iterator<IcebergDataset> getDatasetsIterator() throws IOException {
    return findDatasets().iterator();
  }
  protected IcebergDataset createIcebergDataset(String dbName, String tblName, IcebergTable icebergTable, Properties properties, FileSystem fs) {
    return new IcebergDataset(dbName, tblName, icebergTable, properties, fs);
  }
}
