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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.config.ConfigBuilder;
import org.apache.gobblin.dataset.DatasetConstants;
import org.apache.gobblin.dataset.IterableDatasetFinder;
import org.apache.gobblin.util.HadoopUtils;


/**
 * Finds {@link IcebergDataset}s. Will look for tables in a database using a {@link IcebergCatalog},
 * and creates a {@link IcebergDataset} for each one.
 */
@Slf4j
@RequiredArgsConstructor
public class IcebergDatasetFinder implements IterableDatasetFinder<IcebergDataset> {
  public static final String ICEBERG_DATASET_PREFIX = DatasetConstants.PLATFORM_ICEBERG + ".dataset";
  public static final String DEFAULT_ICEBERG_CATALOG_CLASS = "org.apache.gobblin.data.management.copy.iceberg.IcebergHiveCatalog";
  public static final String ICEBERG_CATALOG_KEY = "catalog";
  /**
   * This is used with a prefix: "{@link IcebergDatasetFinder#ICEBERG_DATASET_PREFIX}" + "." + "( source | destination )" + "." + "{@link IcebergDatasetFinder#ICEBERG_CATALOG_KEY}" + "..."
   * It is an open-ended pattern used to pass arbitrary catalog-scoped properties
   */
  public static final String ICEBERG_CATALOG_CLASS_KEY = "class";
  public static final String ICEBERG_DB_NAME_KEY = "database.name";
  public static final String ICEBERG_TABLE_NAME_KEY = "table.name";
  /** please use source/dest-scoped properties */
  @Deprecated
  public static final String ICEBERG_DB_NAME_LEGACY = ICEBERG_DATASET_PREFIX + "." + ICEBERG_DB_NAME_KEY;
  /** please use source/dest-scoped properties */
  @Deprecated
  public static final String ICEBERG_TABLE_NAME_LEGACY = ICEBERG_DATASET_PREFIX + "." + ICEBERG_TABLE_NAME_KEY;

  public enum CatalogLocation {
    SOURCE,
    DESTINATION;

    /**
     * Provides prefix for configs based on the catalog orientation (source or destination) for catalog-targeted properties
     */
    public String getConfigPrefix() {
      return ICEBERG_DATASET_PREFIX + "." + this.toString().toLowerCase() + ".";
    }
  }

  protected final FileSystem sourceFs;
  private final Properties properties;

  /**
   * Finds all {@link IcebergDataset}s in the file system using the Iceberg Catalog.
   * Both Iceberg database name and table name are mandatory based on current implementation.
   * Later we may explore supporting datasets similar to Hive
   * @return List of {@link IcebergDataset}s in the file system.
   * @throws IOException
   */
  @Override
  public List<IcebergDataset> findDatasets() throws IOException {
    String srcDbName = getLocationQualifiedProperty(properties, CatalogLocation.SOURCE, ICEBERG_DB_NAME_KEY);
    String destDbName = getLocationQualifiedProperty(properties, CatalogLocation.DESTINATION, ICEBERG_DB_NAME_KEY);
    // TODO: eventually remove support for combo (src+dest) iceberg props, in favor of separate source/dest-scoped props; for now, maintain support
    if (StringUtils.isBlank(srcDbName) || StringUtils.isBlank(destDbName)) {
      srcDbName = destDbName = properties.getProperty(ICEBERG_DB_NAME_LEGACY);
    }
    String srcTableName = getLocationQualifiedProperty(properties, CatalogLocation.SOURCE, ICEBERG_TABLE_NAME_KEY);
    String destTableName = getLocationQualifiedProperty(properties, CatalogLocation.DESTINATION, ICEBERG_TABLE_NAME_KEY);
    // TODO: eventually remove support for combo (src+dest) iceberg props, in favor of separate source/dest-scoped props; for now, maintain support
    if (StringUtils.isBlank(srcTableName) || StringUtils.isBlank(destTableName)) {
      srcTableName = destTableName = properties.getProperty(ICEBERG_TABLE_NAME_LEGACY);
    }
    if (StringUtils.isBlank(srcDbName) || StringUtils.isBlank(srcTableName)) {
      throw new IllegalArgumentException(
          String.format("Missing (at least some) IcebergDataset properties - source: ('%s' and '%s') and destination: ('%s' and '%s') "
                  + "or [deprecated!] common/combo: ('%s' and '%s')",
              calcLocationQualifiedPropName(CatalogLocation.SOURCE, ICEBERG_DB_NAME_KEY),
              calcLocationQualifiedPropName(CatalogLocation.SOURCE, ICEBERG_TABLE_NAME_KEY),
              calcLocationQualifiedPropName(CatalogLocation.DESTINATION, ICEBERG_DB_NAME_KEY),
              calcLocationQualifiedPropName(CatalogLocation.DESTINATION, ICEBERG_TABLE_NAME_KEY),
              ICEBERG_DB_NAME_LEGACY,
              ICEBERG_TABLE_NAME_LEGACY));
    }

    IcebergCatalog srcIcebergCatalog = createIcebergCatalog(this.properties, CatalogLocation.SOURCE);
    IcebergCatalog destIcebergCatalog = createIcebergCatalog(this.properties, CatalogLocation.DESTINATION);

    /* Each Iceberg dataset maps to an Iceberg table */
    List<IcebergDataset> matchingDatasets = new ArrayList<>();
    matchingDatasets.add(createIcebergDataset(srcIcebergCatalog, srcDbName, srcTableName, destIcebergCatalog, destDbName, destTableName, this.properties, this.sourceFs));
    log.info("Found {} matching datasets: {} for the (source) '{}.{}' / (dest) '{}.{}'", matchingDatasets.size(),
        matchingDatasets, srcDbName, srcTableName, destDbName, destTableName); // until future support added to specify multiple icebergs, count expected always to be one
    return matchingDatasets;
  }

  @Override
  public Path commonDatasetRoot() {
    return new Path("/");
  }

  @Override
  public Iterator<IcebergDataset> getDatasetsIterator() throws IOException {
    return findDatasets().iterator();
  }

  /**
   * Uses each source and destination {@link IcebergCatalog} to load and verify existence of the respective {@link IcebergTable}
   * Note: the destination side {@link IcebergTable} should be present before initiating replication
   *
   * @return {@link IcebergDataset} with its corresponding source and destination {@link IcebergTable}
   */
  protected IcebergDataset createIcebergDataset(IcebergCatalog sourceIcebergCatalog, String srcDbName, String srcTableName, IcebergCatalog destinationIcebergCatalog, String destDbName, String destTableName, Properties properties, FileSystem fs) throws IOException {
    IcebergTable srcIcebergTable = sourceIcebergCatalog.openTable(srcDbName, srcTableName);
    Preconditions.checkArgument(sourceIcebergCatalog.tableAlreadyExists(srcIcebergTable), String.format("Missing Source Iceberg Table: {%s}.{%s}", srcDbName, srcTableName));
    IcebergTable destIcebergTable = destinationIcebergCatalog.openTable(destDbName, destTableName);
    // TODO: Rethink strategy to enforce dest iceberg table
    Preconditions.checkArgument(destinationIcebergCatalog.tableAlreadyExists(destIcebergTable), String.format("Missing Destination Iceberg Table: {%s}.{%s}", destDbName, destTableName));
    return new IcebergDataset(srcIcebergTable, destIcebergTable, properties, fs);
  }

  protected static IcebergCatalog createIcebergCatalog(Properties properties, CatalogLocation location) throws IOException {
    String catalogPrefix = calcLocationQualifiedPropName(location, ICEBERG_CATALOG_KEY + ".");
    Map<String, String> catalogProperties = buildMapFromPrefixChildren(properties, catalogPrefix);
    // TODO: Filter properties specific to Hadoop
    Configuration configuration = HadoopUtils.getConfFromProperties(properties);
    String icebergCatalogClassName = catalogProperties.getOrDefault(ICEBERG_CATALOG_CLASS_KEY, DEFAULT_ICEBERG_CATALOG_CLASS);
    return IcebergCatalogFactory.create(icebergCatalogClassName, catalogProperties, configuration);
  }

  /** @return property value or `null` */
  protected static String getLocationQualifiedProperty(Properties properties, CatalogLocation location, String relativePropName) {
    return properties.getProperty(calcLocationQualifiedPropName(location, relativePropName));
  }

  /** @return absolute (`location`-qualified) property name for `relativePropName` */
  protected static String calcLocationQualifiedPropName(CatalogLocation location, String relativePropName) {
    return location.getConfigPrefix() + relativePropName;
  }

  /**
   * Filters the properties based on a prefix using {@link ConfigBuilder#loadProps(Properties, String)} and creates a {@link Map}
   */
  protected static Map<String, String> buildMapFromPrefixChildren(Properties properties, String configPrefix) {
    Map<String, String> catalogProperties = new HashMap<>();
    Config config = ConfigBuilder.create().loadProps(properties, configPrefix).build();
    for (Map.Entry<String, ConfigValue> entry : config.entrySet()) {
      catalogProperties.put(entry.getKey(), entry.getValue().unwrapped().toString());
    }
    String catalogUri = config.getString(CatalogProperties.URI);
    Preconditions.checkNotNull(catalogUri, "Provide: {%s} as Catalog Table Service URI is required", configPrefix + "." + CatalogProperties.URI);
    return catalogProperties;
  }
}
