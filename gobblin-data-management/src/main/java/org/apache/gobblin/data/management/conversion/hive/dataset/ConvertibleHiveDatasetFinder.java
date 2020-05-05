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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.gobblin.data.management.conversion.hive.source.HiveAvroToOrcSource;
import org.apache.gobblin.hive.HiveMetastoreClientPool;
import org.apache.gobblin.util.AutoReturnableObject;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.PropertiesUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Table;

import com.typesafe.config.Config;

import org.apache.gobblin.data.management.copy.hive.HiveDatasetFinder;
import org.apache.gobblin.metrics.event.EventSubmitter;


/**
 * A {@link HiveDatasetFinder} to create {@link ConvertibleHiveDataset}s
 */
@Slf4j
public class ConvertibleHiveDatasetFinder extends HiveDatasetFinder {
  protected final boolean cleaningStaging;

  public ConvertibleHiveDatasetFinder(FileSystem fs, Properties properties, EventSubmitter eventSubmitter) throws IOException {
    super(fs, properties, eventSubmitter);
    this.cleaningStaging = getCleaningStaging(properties);
  }

  public ConvertibleHiveDatasetFinder(FileSystem fs, Properties properties, HiveMetastoreClientPool clientPool) throws IOException {
    super(fs, properties, clientPool);
    this.cleaningStaging = getCleaningStaging(properties);;
  }

  private boolean getCleaningStaging(Properties props) {
    String keyPrefix = props.getProperty(HiveDatasetFinder.HIVE_DATASET_CONFIG_PREFIX_KEY, StringUtils.EMPTY);
    String key = keyPrefix.isEmpty()? HiveAvroToOrcSource.CLEAN_STAGING_TABLES_IN_SEARCH :
            keyPrefix + "." + HiveAvroToOrcSource.CLEAN_STAGING_TABLES_IN_SEARCH;
    return PropertiesUtils.getPropAsBoolean(props, key, HiveAvroToOrcSource.DEFAULT_CLEAN_STAGING_FROM_BEGINNING);
  }

  private boolean getCleaningStaging(Config config) {
    return ConfigUtils.getBoolean(config, HiveAvroToOrcSource.CLEAN_STAGING_TABLES_IN_SEARCH,
            this.cleaningStaging);
  }

  protected ConvertibleHiveDataset createHiveDataset(Table table, Config config) {
    ConvertibleHiveDataset hiveDataset = new ConvertibleHiveDataset(super.fs, super.clientPool, new org.apache.hadoop.hive.ql.metadata.Table(table),
        this.properties, config);

    // each dataset can control if it needs to clean up the staging tables
    boolean cleanUpStg = getCleaningStaging(config);

    if (cleanUpStg) {
      // nested
      Optional<ConvertibleHiveDataset.ConversionConfig> nestedConfig = hiveDataset
              .getConversionConfigForFormat(HiveAvroToOrcSource.OrcFormats.NESTED_ORC.getConfigPrefix());

      // flattened
      Optional<ConvertibleHiveDataset.ConversionConfig> flattenedConfig = hiveDataset
              .getConversionConfigForFormat(HiveAvroToOrcSource.OrcFormats.FLATTENED_ORC.getConfigPrefix());

      List<String[]> tablesRequireCleanup = new ArrayList<>();
      if (nestedConfig.isPresent()) {
        String nestedStagingName = nestedConfig.get().getDestinationStagingTableName();
        String dbName = nestedConfig.get().getDestinationDbName();
        tablesRequireCleanup.add(new String[]{dbName, nestedStagingName});
        log.info("Previous avro2orc might have generated nested staging table {}, will search if any exists and clean up.",
                nestedStagingName);
      }

      if (flattenedConfig.isPresent()) {
        String flattenStagingName = flattenedConfig.get().getDestinationStagingTableName();
        String dbName = flattenedConfig.get().getDestinationDbName();
        tablesRequireCleanup.add(new String[]{dbName, flattenStagingName});
        log.info("Previous avro2orc might have generated flatten staging table name {}, will search if any exists and clean up.",
                flattenStagingName);
      }

      try (AutoReturnableObject<IMetaStoreClient> client = clientPool.getClient()) {
        for (String[] dbAndTable : tablesRequireCleanup) {
          String tablePattern = dbAndTable[1] + "_*";
          try {
            List<String> stgTables = client.get().getTables(dbAndTable[0], tablePattern);
            for (String stgTable: stgTables) {
              try {
                log.info("Cleaning {}.{}", dbAndTable[0], stgTable);
                client.get().dropTable(dbAndTable[0], stgTable, false, false);
              } catch (Exception e) {
                log.warn("Cannot clean staging table {}.{}", dbAndTable[0], stgTable);
              }
            }
          } catch (Exception e) {
            log.warn("Cannot get staging tables from {}.{}", dbAndTable[0], tablePattern);
          }
        }
      } catch (Exception e) {
        Throwables.propagate(e);
      }
    }

    return hiveDataset;
  }
}
