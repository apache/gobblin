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

package org.apache.gobblin.data.management.conversion.hive.entities;

import java.util.List;
import java.util.Properties;

import org.apache.gobblin.data.management.copy.hive.HiveDataset;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.hadoop.hive.ql.metadata.Table;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.typesafe.config.Config;

import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Data;


/**
 * Contains metadata associated with a stageable table.
 *
 * This class contains information about two Hive tables: a final destination table and a staging table. The staging
 * table is used as temporary storage during job run to aid with consistency of the final destination table.
 */
@Data
@AllArgsConstructor
public class StageableTableMetadata {

  public static final String DESTINATION_TABLE_KEY = "destination.tableName";
  public static final String DESTINATION_DB_KEY = "destination.dbName";
  public static final String DESTINATION_DATA_PATH_KEY = "destination.dataPath";
  public static final String DESTINATION_TABLE_PROPERTIES_LIST_KEY = "destination.tableProperties";
  public static final String CLUSTER_BY_KEY = "clusterByList";
  public static final String NUM_BUCKETS_KEY = "numBuckets";
  public static final String EVOLUTION_ENABLED = "evolution.enabled";
  public static final String ROW_LIMIT_KEY = "rowLimit";
  public static final String HIVE_VERSION_KEY = "hiveVersion";
  public static final String HIVE_RUNTIME_PROPERTIES_LIST_KEY = "hiveRuntimeProperties";
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
  public static final String SOURCE_DATA_PATH_IDENTIFIER_KEY = "source.dataPathIdentifier";


  /** Table name of the destination table. */
  private final String destinationTableName;
  /** Table name of the staging table. */
  private final String destinationStagingTableName;
  /** Name of db for destination name. */
  private final String destinationDbName;
  /** Path where files of the destination table should be located. */
  private final String destinationDataPath;
  /** Table properties of destination table. */
  private final Properties destinationTableProperties;
  /** List of columns to cluster by. */
  private final List<String> clusterBy;
  /** Number of buckets in destination table. */
  private final Optional<Integer> numBuckets;
  private final Properties hiveRuntimeProperties;
  private final boolean evolutionEnabled;
  private final Optional<Integer> rowLimit;
  private final List<String> sourceDataPathIdentifier;

  public StageableTableMetadata(Config config, @Nullable Table referenceTable) {
    Preconditions.checkArgument(config.hasPath(DESTINATION_TABLE_KEY), String.format("Key %s is not specified", DESTINATION_TABLE_KEY));
    Preconditions.checkArgument(config.hasPath(DESTINATION_DB_KEY), String.format("Key %s is not specified", DESTINATION_DB_KEY));
    Preconditions.checkArgument(config.hasPath(DESTINATION_DATA_PATH_KEY),
        String.format("Key %s is not specified", DESTINATION_DATA_PATH_KEY));

    // Required
    this.destinationTableName = referenceTable == null ? config.getString(DESTINATION_TABLE_KEY)
        : HiveDataset.resolveTemplate(config.getString(DESTINATION_TABLE_KEY), referenceTable);
    this.destinationStagingTableName = String.format("%s_%s", this.destinationTableName, "staging"); // Fixed and non-configurable
    this.destinationDbName = referenceTable == null ? config.getString(DESTINATION_DB_KEY)
        : HiveDataset.resolveTemplate(config.getString(DESTINATION_DB_KEY), referenceTable);
    this.destinationDataPath = referenceTable == null ? config.getString(DESTINATION_DATA_PATH_KEY)
        : HiveDataset.resolveTemplate(config.getString(DESTINATION_DATA_PATH_KEY), referenceTable);

    // Optional
    this.destinationTableProperties =
        convertKeyValueListToProperties(ConfigUtils.getStringList(config, DESTINATION_TABLE_PROPERTIES_LIST_KEY));
    this.clusterBy = ConfigUtils.getStringList(config, CLUSTER_BY_KEY);
    this.numBuckets = Optional.fromNullable(ConfigUtils.getInt(config, NUM_BUCKETS_KEY, null));

    this.hiveRuntimeProperties =
        convertKeyValueListToProperties(ConfigUtils.getStringList(config, HIVE_RUNTIME_PROPERTIES_LIST_KEY));
    this.evolutionEnabled = ConfigUtils.getBoolean(config, EVOLUTION_ENABLED, false);
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
