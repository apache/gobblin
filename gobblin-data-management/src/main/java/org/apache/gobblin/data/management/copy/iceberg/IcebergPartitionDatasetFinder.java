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
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;

import com.google.common.base.Preconditions;

import lombok.extern.slf4j.Slf4j;

/**
 * Finder class for locating and creating partitioned Iceberg datasets.
 * <p>
 * This class extends {@link IcebergDatasetFinder} and provides functionality to create
 * {@link IcebergPartitionDataset} instances based on the specified source and destination Iceberg catalogs.
 * </p>
 */
@Slf4j
public class IcebergPartitionDatasetFinder extends IcebergDatasetFinder {
  public IcebergPartitionDatasetFinder(FileSystem sourceFs, Properties properties) {
    super(sourceFs, properties);
  }

  /**
   * Finds the {@link IcebergPartitionDataset}s in the file system using the Iceberg Catalog. Both Iceberg database name and table
   * name are mandatory based on current implementation.
   * <p>
   * Overriding this method to put a check whether source and destination db & table names are passed in the properties as separate values
   * </p>
   * @return List of {@link IcebergPartitionDataset}s in the file system.
   * @throws IOException if there is an error while finding the datasets.
   */
  @Override
  public List<IcebergDataset> findDatasets() throws IOException {
    String srcDbName = getLocationQualifiedProperty(this.properties, CatalogLocation.SOURCE, ICEBERG_DB_NAME_KEY);
    String destDbName = getLocationQualifiedProperty(this.properties, CatalogLocation.DESTINATION, ICEBERG_DB_NAME_KEY);
    String srcTableName = getLocationQualifiedProperty(this.properties, CatalogLocation.SOURCE, ICEBERG_TABLE_NAME_KEY);
    String destTableName = getLocationQualifiedProperty(this.properties, CatalogLocation.DESTINATION, ICEBERG_TABLE_NAME_KEY);

    if (StringUtils.isBlank(srcDbName) || StringUtils.isBlank(destDbName) || StringUtils.isBlank(srcTableName)
        || StringUtils.isBlank(destTableName)) {
      String errorMsg = String.format("Missing (at least some) IcebergDataset properties - source: ('%s' and '%s') and destination: ('%s' and '%s') ",
          srcDbName, srcTableName, destDbName, destTableName);
      log.error(errorMsg);
      throw new IllegalArgumentException(errorMsg);
    }

    IcebergCatalog srcIcebergCatalog = createIcebergCatalog(this.properties, CatalogLocation.SOURCE);
    IcebergCatalog destIcebergCatalog = createIcebergCatalog(this.properties, CatalogLocation.DESTINATION);

    return Collections.singletonList(createIcebergDataset(
        srcIcebergCatalog, srcDbName, srcTableName,
        destIcebergCatalog, destDbName, destTableName,
        this.properties, this.sourceFs
    ));
  }

 /**
 * Creates an {@link IcebergPartitionDataset} instance for the specified source and destination Iceberg tables.
 */
  @Override
  protected IcebergDataset createIcebergDataset(IcebergCatalog sourceIcebergCatalog, String srcDbName, String srcTableName, IcebergCatalog destinationIcebergCatalog, String destDbName, String destTableName, Properties properties, FileSystem fs) throws IOException {
    IcebergTable srcIcebergTable = sourceIcebergCatalog.openTable(srcDbName, srcTableName);
    Preconditions.checkArgument(sourceIcebergCatalog.tableAlreadyExists(srcIcebergTable),
        String.format("Missing Source Iceberg Table: {%s}.{%s}", srcDbName, srcTableName));
    IcebergTable destIcebergTable = destinationIcebergCatalog.openTable(destDbName, destTableName);
    Preconditions.checkArgument(destinationIcebergCatalog.tableAlreadyExists(destIcebergTable),
        String.format("Missing Destination Iceberg Table: {%s}.{%s}", destDbName, destTableName));
//    TODO: Add Validator for source and destination tables later
//    TableMetadata srcTableMetadata = srcIcebergTable.accessTableMetadata();
//    TableMetadata destTableMetadata = destIcebergTable.accessTableMetadata();
//    IcebergTableMetadataValidator.validateSourceAndDestinationTablesMetadata(srcTableMetadata, destTableMetadata);
    return new IcebergPartitionDataset(srcIcebergTable, destIcebergTable, properties, fs, getConfigShouldCopyMetadataPath(properties));
  }
}