/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */
package gobblin.data.management.conversion.hive.dataset;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import lombok.Getter;
import lombok.ToString;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.ql.metadata.Table;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

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
 */
@ToString
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
  public ConvertibleHiveDataset(FileSystem fs, HiveMetastoreClientPool clientPool, Table table, Config config) {
    super(fs, clientPool, table, config);

    Preconditions.checkArgument(config.hasPath(DESTINATION_CONVERSION_FORMATS_KEY), String.format(
        "Atleast one destination format should be specified at %s. If you do not intend to convert this dataset set %s to true",
        DESTINATION_CONVERSION_FORMATS_KEY, HiveDatasetFinder.HIVE_DATASET_IS_BLACKLISTED_KEY));

    // value for DESTINATION_CONVERSION_FORMATS_KEY can be a TypeSafe list or a comma separated list of string
    this.destFormats = Sets.newHashSet(ConfigUtils.getStringList(config, DESTINATION_CONVERSION_FORMATS_KEY));

    // For each format create ConversionConfig and store it in a Map<format,conversionConfig>
    this.destConversionConfigs = Maps.newHashMap();

    for (String format : this.destFormats) {
      if (config.hasPath(format)) {
        this.destConversionConfigs.put(format, new ConversionConfig(config.getConfig(format), table, format));

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
   *    <li>Any properties with a prefix of {@value #HIVE_RUNTIME_PROPERTIES_KEY_PREFIX} will be available at
   *    {@link #getHiveRuntimeProperties()} without the prefix.
   *  </ul>
   * <p>
   */
  @Getter
  @ToString
  public static class ConversionConfig {
    public static final String DESTINATION_TABLE_KEY = "destination.tableName";
    public static final String DESTINATION_DB_KEY = "destination.dbName";
    public static final String DESTINATION_DATA_PATH_KEY = "destination.dataPath";
    public static final String CLUSTER_BY_KEY = "clusterByList";
    public static final String NUM_BUCKETS_KEY = "numBuckets";
    public static final String EVOLUTION_ENABLED = "evolution.enabled";
    public static final String ROW_LIMIT_KEY = "rowLimit";
    public static final String HIVE_VERSION_KEY = "hiveVersion";
    private static final String HIVE_RUNTIME_PROPERTIES_KEY_PREFIX = "hiveRuntime";

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
    private final String destinationStagingTableName;
    private final String destinationDbName;
    private final String destinationDataPath;
    private final List<String> clusterBy;
    private final Optional<Integer> numBuckets;
    private final Properties hiveRuntimeProperties;
    private final boolean evolutionEnabled;
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
      this.clusterBy = ConfigUtils.getStringList(config, CLUSTER_BY_KEY);
      this.numBuckets = Optional.fromNullable(ConfigUtils.getInt(config, NUM_BUCKETS_KEY, null));
      this.hiveRuntimeProperties = ConfigUtils
          .configToProperties(ConfigUtils.getConfig(config, HIVE_RUNTIME_PROPERTIES_KEY_PREFIX, ConfigFactory.empty()));
      this.evolutionEnabled = ConfigUtils.getBoolean(config, EVOLUTION_ENABLED, false);
      this.rowLimit = Optional.fromNullable(ConfigUtils.getInt(config, ROW_LIMIT_KEY, null));
      this.sourceDataPathIdentifier = ConfigUtils.getStringList(config, SOURCE_DATA_PATH_IDENTIFIER_KEY);
    }
  }

}
