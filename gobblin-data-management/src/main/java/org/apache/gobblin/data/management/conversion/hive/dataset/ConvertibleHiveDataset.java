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
package org.apache.gobblin.data.management.conversion.hive.dataset;

import java.util.Map;
import java.util.Properties;
import java.util.Set;

import lombok.Getter;
import lombok.ToString;

import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.ql.metadata.Table;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;

import org.apache.gobblin.data.management.conversion.hive.entities.StageableTableMetadata;
import org.apache.gobblin.data.management.copy.hive.HiveDataset;
import org.apache.gobblin.data.management.copy.hive.HiveDatasetFinder;
import org.apache.gobblin.hive.HiveMetastoreClientPool;
import org.apache.gobblin.util.ConfigUtils;


/**
 * <p>
 * A {@link HiveDataset} that can be converted from one source format to several destination formats.
 * This class holds the {@link ConversionConfig}s required for conversion into each
 * destination format. The {@link ConversionConfig} for a destination format can be accessed by calling {@link #getConversionConfigForFormat(String)}.
 * </p>
 *
 * <p>
 * <b>Instantiation</b>
 * <ul>
 *  <li> The constructor takes in a dataset {@link Config} which MUST have a comma separated list of destination formats at key,
 *  {@value #DESTINATION_CONVERSION_FORMATS_KEY}
 *  <li> Conversion configuration for a format can be set by using this destination format as prefix.
 *  <li> E.g. If {@value #DESTINATION_CONVERSION_FORMATS_KEY}=flattenedOrc,nestedOrc.<br>
 *  The destination table name for flattened ORC is set at flattenedOrc.tableName<br>
 *  And the destination table name for nested ORC is set at nestedOrc.tableName
 * </ul>
 * </p>
 * @see ConversionConfig
 */
@ToString
@Slf4j
public class ConvertibleHiveDataset extends HiveDataset {

  public static final String DESTINATION_CONVERSION_FORMATS_KEY = "destinationFormats";

  // Destination formats
  @Getter
  private final Set<String> destFormats;

  // Mapping for destination format to it's Conversion config
  private final Map<String, ConversionConfig> destConversionConfigs;

  /**
   * <ul>
   *  <li> The constructor takes in a dataset {@link Config} which MUST have a comma separated list of destination formats at key,
   *  {@value #DESTINATION_CONVERSION_FORMATS_KEY}
   *  <li> Conversion configuration for a format can be set by using destination format as prefix.
   *  <li> E.g. If {@value #DESTINATION_CONVERSION_FORMATS_KEY}=flattenedOrc,nestedOrc.<br>
   *  The destination table name for flattened ORC is set at flattenedOrc.tableName<br>
   *  And the destination table name for nested ORC is set at nestedOrc.tableName
   * </ul>
   * @param fs
   * @param clientPool
   * @param table
   * @param config
   */
  public ConvertibleHiveDataset(FileSystem fs, HiveMetastoreClientPool clientPool, Table table, Properties jobProps, Config config) {
    super(fs, clientPool, table, jobProps, config);

    Preconditions.checkArgument(config.hasPath(DESTINATION_CONVERSION_FORMATS_KEY), String.format(
        "Atleast one destination format should be specified at %s.%s. If you do not intend to convert this dataset set %s.%s to true",
        super.properties.getProperty(HiveDatasetFinder.HIVE_DATASET_CONFIG_PREFIX_KEY, ""),
        DESTINATION_CONVERSION_FORMATS_KEY,
        super.properties.getProperty(HiveDatasetFinder.HIVE_DATASET_CONFIG_PREFIX_KEY, ""),
        HiveDatasetFinder.HIVE_DATASET_IS_BLACKLISTED_KEY));

    // value for DESTINATION_CONVERSION_FORMATS_KEY can be a TypeSafe list or a comma separated list of string
    this.destFormats = Sets.newHashSet(ConfigUtils.getStringList(this.datasetConfig, DESTINATION_CONVERSION_FORMATS_KEY));

    // For each format create ConversionConfig and store it in a Map<format,conversionConfig>
    this.destConversionConfigs = Maps.newHashMap();

    for (String format : this.destFormats) {
      if (this.datasetConfig.hasPath(format)) {
        log.debug("Found desination format: " + format);
        this.destConversionConfigs.put(format, new ConversionConfig(this.datasetConfig.getConfig(format), table, format));

      }
    }
  }

  /**
   * Return the {@link ConversionConfig} for a destination format if available. If not return {@link Optional#absent()}
   * @param format for which {@link ConversionConfig} needs to be returned
   */
  public Optional<ConversionConfig> getConversionConfigForFormat(String format) {
    return Optional.fromNullable(this.destConversionConfigs.get(format));
  }

  /**
   * The Conversion configuration for converting from source format to each destination format.
   * <p>
   * <b>Required properties</b>
   *  <ul>
   *    <li>{@value #DESTINATION_DB_KEY}
   *    <li>{@value #DESTINATION_TABLE_KEY}
   *    <li>{@value #DESTINATION_DATA_PATH_KEY}
   *  </ul>
   * <b>Optional properties</b>
   *  <ul>
   *    <li>{@value #CLUSTER_BY_KEY}
   *    <li>{@value #NUM_BUCKETS_KEY}
   *    <li>{@value #HIVE_RUNTIME_PROPERTIES_LIST_KEY} can be used to provide a list of hive properties to be set before
   *    conversion. The value should can be an array of keys and values or a comma separated string of keys and values.
   *    E.g. [key1,value1,key2,value2] or key1,value1,key2,value2
   *    <li>{@value #DESTINATION_TABLE_PROPERTIES_LIST_KEY} can be used to provide a list of table properties to be set
   *    on the destination table. The value should can be an array of keys and values or a comma separated string of keys and values.
   *    E.g. [key1,value1,key2,value2] or key1,value1,key2,value2
   *  </ul>
   * <p>
   */
  @Getter
  @ToString
  public static class ConversionConfig extends StageableTableMetadata {
    public static final String DESTINATION_VIEW_KEY = "destination.viewName";
    public static final String UPDATE_VIEW_ALWAYS_ENABLED = "updateViewAlways.enabled";

    private final String destinationFormat;
    // destinationViewName : If specified view with 'destinationViewName' is created if not already exists over destinationTableName
    private final Optional<String> destinationViewName;
     // updateViewAlwaysEnabled: If false 'destinationViewName' is only updated when schema evolves; if true 'destinationViewName'
    // ... is always updated (everytime publish happens)
    private final boolean updateViewAlwaysEnabled;

    private ConversionConfig(Config config, Table table, String destinationFormat) {
      super(config, table);

      // Required
      this.destinationFormat = destinationFormat;

      // Optional
      this.destinationViewName = Optional.fromNullable(resolveTemplate(ConfigUtils.getString(config, DESTINATION_VIEW_KEY, null), table));
      this.updateViewAlwaysEnabled = ConfigUtils.getBoolean(config, UPDATE_VIEW_ALWAYS_ENABLED, true);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }

      ConversionConfig that = (ConversionConfig) o;

      if (isUpdateViewAlwaysEnabled() != that.isUpdateViewAlwaysEnabled()) {
        return false;
      }
      if (!getDestinationFormat().equals(that.getDestinationFormat())) {
        return false;
      }
      return getDestinationViewName().equals(that.getDestinationViewName());
    }

    @Override
    public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + getDestinationFormat().hashCode();
      result = 31 * result + getDestinationViewName().hashCode();
      result = 31 * result + (isUpdateViewAlwaysEnabled() ? 1 : 0);
      return result;
    }
  }

}
