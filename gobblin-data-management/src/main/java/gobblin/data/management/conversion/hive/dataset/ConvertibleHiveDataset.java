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

import java.util.HashSet;
import java.util.Properties;

import lombok.Getter;
import lombok.ToString;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.ql.metadata.Table;

import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;

import gobblin.data.management.copy.hive.HiveDataset;
import gobblin.hive.HiveMetastoreClientPool;
import gobblin.util.ConfigUtils;

/**
 * A {@link HiveDataset} that can be converted to another {@link HiveDataset}
 */
@Getter
@ToString
public class ConvertibleHiveDataset extends HiveDataset {

  public static final String DESTINATION_TABLE_KEY = "destination.tableName";
  public static final String DESTINATION_DB_KEY = "destination.dbName";
  public static final String DESTINATION_DATA_PATH_KEY = "destination.dataPath";
  public static final String DESTINATION_CONVERSION_FORMATS_KEY = "destination.formats";

  private static final String DB_NAME_TEMPLATE = "{DB}";
  private static final String TABLE_NAME_TEMPLATE = "{TABLE}";

  private static final String HIVE_RUNTIME_PROPERTIES_KEY_PREFIX = "hiveRuntime";
  private final Optional<String> destinationTableName;
  private final Optional<String> destinationDbName;
  private final Optional<String> destinationDataPath;
  private final Optional<HashSet<String>> formats;
  private final Properties hiveProperties;

  public ConvertibleHiveDataset(FileSystem fs, HiveMetastoreClientPool clientPool, Table table, Config config) {
    super(fs, clientPool, table, config);
    this.destinationTableName = Optional.fromNullable(resolveTemplate(ConfigUtils.getString(config, DESTINATION_TABLE_KEY, null), table));
    this.destinationDbName = Optional.fromNullable(resolveTemplate(ConfigUtils.getString(config, DESTINATION_DB_KEY, null), table));
    this.destinationDataPath = Optional.fromNullable(resolveTemplate(ConfigUtils.getString(config, DESTINATION_DATA_PATH_KEY, null), table));

    if (config.hasPath(DESTINATION_CONVERSION_FORMATS_KEY)) {
      this.formats = Optional.of(Sets.<String> newHashSet());
      try {
        this.formats.get().addAll(config.getStringList(DESTINATION_CONVERSION_FORMATS_KEY));
      } catch (ConfigException.WrongType e) {
        Splitter tokenSplitter = Splitter.on(",").omitEmptyStrings().trimResults();
        this.formats.get().addAll(tokenSplitter.splitToList(config.getString(DESTINATION_CONVERSION_FORMATS_KEY)));
      }
    } else {
      this.formats = Optional.absent();
    }

    this.hiveProperties = ConfigUtils.configToProperties(ConfigUtils.getConfig(config, HIVE_RUNTIME_PROPERTIES_KEY_PREFIX, ConfigFactory.empty()));

  }

  /**
   * Resolve {@value #DB_NAME_TEMPLATE} and {@value #TABLE_NAME_TEMPLATE} in <code>rawString</code> to {@link Table#getDbName()}
   * and {@link Table#getTableName()}
   */
  private static String resolveTemplate(String rawString, Table table) {
    return StringUtils.replaceEach(rawString, new String[] { DB_NAME_TEMPLATE, TABLE_NAME_TEMPLATE }, new String[] { table.getDbName(), table.getTableName() });
  }
}
