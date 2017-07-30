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
package gobblin.data.management.conversion.hive.dataset;

import java.util.List;
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

import gobblin.data.management.copy.hive.HiveDataset;
import gobblin.data.management.copy.hive.HiveDatasetFinder;
import gobblin.hive.HiveMetastoreClientPool;
import gobblin.util.ConfigUtils;


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
  public static class ConversionConfig {
    public static final String DESTINATION_TABLE_KEY = "destination.tableName";
    public static final String DESTINATION_VIEW_KEY = "destination.viewName";
    public static final String DESTINATION_DB_KEY = "destination.dbName";
    public static final String DESTINATION_DATA_PATH_KEY = "destination.dataPath";
    public static final String DESTINATION_TABLE_PROPERTIES_LIST_KEY = "destination.tableProperties";
    public static final String CLUSTER_BY_KEY = "clusterByList";
    public static final String NUM_BUCKETS_KEY = "numBuckets";
    public static final String EVOLUTION_ENABLED = "evolution.enabled";
    public static final String UPDATE_VIEW_ALWAYS_ENABLED = "updateViewAlways.enabled";
    public static final String ROW_LIMIT_KEY = "rowLimit";
    public static final String HIVE_VERSION_KEY = "hiveVersion";
    private static final String HIVE_RUNTIME_PROPERTIES_LIST_KEY = "hiveRuntimeProperties";

    /***
     * Comma separated list of string that should be used as a prefix for destination partition directory name
     * ... (if present in the location path string of source partition)
     *
     * This is helpful in roll-up / compaction scenarios, where you don't want queries in flight to fail.
     *
     * Scenario without this property:
     * - Source partition: datepartition=2016-01-01-00 with path /foo/bar/hourly/2016/01/01/00 is available for
     *   processing
     * - Source partition is processed and published to destination table as: /foo/bar_orc/datepartition=2016-01-01-00
     *
     * - Source partition: datepartition=2016-01-01-00 with path /foo/bar/daily/2016/01/01/00 is available again for
     *   processing (due to roll-up / compaction of hourly data for 2016-01-01 into same partition)
     * - Source partition is processed and published to destination table as: /foo/bar_orc/datepartition=2016-01-01-00
     *   (previous data is overwritten and any queries in flight fail)
     *
     * Same scenario with this property set to "hourly,daily":
     * - Source partition: datepartition=2016-01-01-00 with path /foo/bar/hourly/2016/01/01/00 is available for
     *   processing
     * - Source partition is processed and published to destination table as: /foo/bar_orc/hourly_datepartition=2016-01-01-00
     *   (Note: "hourly_" is prefixed to destination partition directory name because source partition path contains
     *   "hourly" substring)
     *
     * - Source partition: datepartition=2016-01-01-00 with path /foo/bar/daily/2016/01/01/00 is available again for
     *   processing (due to roll-up / compaction of hourly data for 2016-01-01 into same partition)
     * - Source partition is processed and published to destination table as: /foo/bar_orc/daily_datepartition=2016-01-01-00
     *   (Note: "daily_" is prefixed to destination partition directory name, because source partition path contains
     *   "daily" substring)
     * - Any running queries are not impacted since data is not overwritten and hourly_datepartition=2016-01-01-00
     *   directory continues to exist
     *
     * Notes:
     * - This however leaves the responsibility of cleanup of previous destination partition directory on retention or
     *   other such independent module, since in the above case hourly_datepartition=2016-01-01-00 dir will not be deleted
     * - Directories can still be overwritten if they resolve to same destination partition directory name, such as
     *   re-processing / backfill of daily partition will overwrite daily_datepartition=2016-01-01-00 directory
     */
    private static final String SOURCE_DATA_PATH_IDENTIFIER_KEY = "source.dataPathIdentifier";

    private final String destinationFormat;
    private final String destinationTableName;
    // destinationViewName : If specified view with 'destinationViewName' is created if not already exists over destinationTableName
    private final Optional<String> destinationViewName;
    private final String destinationStagingTableName;
    private final String destinationDbName;
    private final String destinationDataPath;
    private final Properties destinationTableProperties;
    private final List<String> clusterBy;
    private final Optional<Integer> numBuckets;
    private final Properties hiveRuntimeProperties;
    private final boolean evolutionEnabled;
    // updateViewAlwaysEnabled: If false 'destinationViewName' is only updated when schema evolves; if true 'destinationViewName'
    // ... is always updated (everytime publish happens)
    private final boolean updateViewAlwaysEnabled;
    private final Optional<Integer> rowLimit;
    private final List<String> sourceDataPathIdentifier;

    private ConversionConfig(Config config, Table table, String destinationFormat) {

      Preconditions.checkArgument(config.hasPath(DESTINATION_TABLE_KEY), String.format("Key %s.%s is not specified", destinationFormat, DESTINATION_TABLE_KEY));
      Preconditions.checkArgument(config.hasPath(DESTINATION_DB_KEY), String.format("Key %s.%s is not specified", destinationFormat, DESTINATION_DB_KEY));
      Preconditions.checkArgument(config.hasPath(DESTINATION_DATA_PATH_KEY),
          String.format("Key %s.%s is not specified", destinationFormat, DESTINATION_DATA_PATH_KEY));

      // Required
      this.destinationFormat = destinationFormat;
      this.destinationTableName = resolveTemplate(config.getString(DESTINATION_TABLE_KEY), table);
      this.destinationStagingTableName = String.format("%s_%s", this.destinationTableName, "staging"); // Fixed and non-configurable
      this.destinationDbName = resolveTemplate(config.getString(DESTINATION_DB_KEY), table);
      this.destinationDataPath = resolveTemplate(config.getString(DESTINATION_DATA_PATH_KEY), table);

      // Optional
      this.destinationViewName = Optional.fromNullable(resolveTemplate(ConfigUtils.getString(config, DESTINATION_VIEW_KEY, null), table));
      this.destinationTableProperties =
          convertKeyValueListToProperties(ConfigUtils.getStringList(config, DESTINATION_TABLE_PROPERTIES_LIST_KEY));
      this.clusterBy = ConfigUtils.getStringList(config, CLUSTER_BY_KEY);
      this.numBuckets = Optional.fromNullable(ConfigUtils.getInt(config, NUM_BUCKETS_KEY, null));

      this.hiveRuntimeProperties =
          convertKeyValueListToProperties(ConfigUtils.getStringList(config, HIVE_RUNTIME_PROPERTIES_LIST_KEY));
      this.evolutionEnabled = ConfigUtils.getBoolean(config, EVOLUTION_ENABLED, false);
      this.updateViewAlwaysEnabled = ConfigUtils.getBoolean(config, UPDATE_VIEW_ALWAYS_ENABLED, true);
      this.rowLimit = Optional.fromNullable(ConfigUtils.getInt(config, ROW_LIMIT_KEY, null));
      this.sourceDataPathIdentifier = ConfigUtils.getStringList(config, SOURCE_DATA_PATH_IDENTIFIER_KEY);
    }

    private Properties convertKeyValueListToProperties(List<String> keyValueList) {
      Preconditions.checkArgument(keyValueList.size() % 2 == 0, String.format(
          "The list %s does not have equal number of keys and values. Size %s", keyValueList, keyValueList.size()));
      Properties props = new Properties();
      for (int i = 0; i < keyValueList.size(); i += 2) {
        String key = keyValueList.get(i);
        String value = keyValueList.get(i + 1);
        props.put(key, value);
      }
      return props;
    }
  }

}
