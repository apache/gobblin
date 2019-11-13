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

package org.apache.gobblin.service.modules.dataset;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.GlobPattern;

import com.google.common.base.Splitter;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;

import lombok.EqualsAndHashCode;

import org.apache.gobblin.data.management.copy.hive.HiveCopyEntityHelper;
import org.apache.gobblin.data.management.version.finder.DatePartitionHiveVersionFinder;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.PathUtils;


/**
 * As of now, {@link HiveDatasetDescriptor} has same implementation as that of {@link SqlDatasetDescriptor}.
 * Fields {@link HiveDatasetDescriptor#isPartitioned}, {@link HiveDatasetDescriptor#partitionColumn} and
 * {@link HiveDatasetDescriptor#partitionFormat} are used for methods 'equals' and 'hashCode'.
 */
@EqualsAndHashCode (callSuper = true)
public class HiveDatasetDescriptor extends SqlDatasetDescriptor {
  static final String IS_PARTITIONED_KEY = "isPartitioned";
  static final String PARTITION_COLUMN = "partition.column";
  static final String PARTITION_FORMAT = "partition.format";
  static final String CONFLICT_POLICY = "conflict.policy";
  static final String WHITELIST_TABLES = "whitelist.tables";
  private final boolean isPartitioned;
  private final String partitionColumn;
  private final String partitionFormat;
  private final String conflictPolicy;

  public HiveDatasetDescriptor(Config config) throws IOException {
    super(config);
    this.isPartitioned = ConfigUtils.getBoolean(config, IS_PARTITIONED_KEY, true);

    if (isPartitioned) {
      partitionColumn = ConfigUtils.getString(config, PARTITION_COLUMN, DatePartitionHiveVersionFinder.DEFAULT_PARTITION_KEY_NAME);
      partitionFormat = ConfigUtils.getString(config, PARTITION_FORMAT, DatePartitionHiveVersionFinder.DEFAULT_PARTITION_VALUE_DATE_TIME_PATTERN);
      conflictPolicy = HiveCopyEntityHelper.ExistingEntityPolicy.REPLACE_PARTITIONS.name();
    } else {
      partitionColumn = "";
      partitionFormat = "";
      conflictPolicy = HiveCopyEntityHelper.ExistingEntityPolicy.REPLACE_TABLE.name();
    }
    this.setRawConfig(this.getRawConfig()
        .withValue(CONFLICT_POLICY, ConfigValueFactory.fromAnyRef(conflictPolicy))
        .withValue(PARTITION_COLUMN, ConfigValueFactory.fromAnyRef(partitionColumn))
        .withValue(PARTITION_FORMAT, ConfigValueFactory.fromAnyRef(partitionFormat))
        .withValue(WHITELIST_TABLES, ConfigValueFactory.fromAnyRef(createWhitelistedTables())
        ));
  }

  private String createWhitelistedTables() {
    return this.tableName.replace(',', '|');
  }

  @Override
  protected boolean isPlatformValid() {
    return "hive".equalsIgnoreCase(getPlatform());
  }

  @Override
  protected boolean isPathContaining(DatasetDescriptor other) {
    String otherPath = other.getPath();
    if (otherPath == null) {
      return false;
    }

    if (this.isPartitioned != ((HiveDatasetDescriptor) other).isPartitioned) {
      return false;
    }

    //Extract the dbName and tableName from otherPath
    List<String> parts = Splitter.on(SEPARATION_CHAR).splitToList(otherPath);
    if (parts.size() != 2) {
      return false;
    }

    String otherDbName = parts.get(0);
    String otherTableNames = parts.get(1);

    if(!Pattern.compile(this.databaseName).matcher(otherDbName).matches()) {
      return false;
    }

    List<String> tables = Splitter.on(",").splitToList(otherTableNames);
    for (String table : tables) {
      if (!isPathContaining(table)) {
        return false;
      }
    }
    return true;
  }

  private boolean isPathContaining(String tableName) {
    if (tableName == null) {
      return false;
    }

    if (PathUtils.GLOB_TOKENS.matcher(tableName).find()) {
      return false;
    }

    GlobPattern globPattern = new GlobPattern(this.tableName);
    return globPattern.matches(tableName);
  }
}
