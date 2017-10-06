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

package org.apache.gobblin.example.hivematerializer;

import java.io.IOException;
import java.util.List;

import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.data.management.conversion.hive.entities.StageableTableMetadata;
import org.apache.gobblin.data.management.conversion.hive.materializer.HiveMaterializer;
import org.apache.gobblin.data.management.conversion.hive.task.HiveConverterUtils;
import org.apache.gobblin.data.management.conversion.hive.task.HiveTask;
import org.apache.gobblin.data.management.copy.hive.HiveDataset;
import org.apache.gobblin.hive.HiveMetastoreClientPool;
import org.apache.gobblin.source.Source;
import org.apache.gobblin.source.extractor.Extractor;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.util.AutoReturnableObject;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.HadoopUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.thrift.TException;

import com.google.api.client.repackaged.com.google.common.base.Splitter;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;
import static org.apache.gobblin.data.management.copy.hive.HiveDatasetFinder.*;


@Slf4j

/**
 * A sample source showing how to create work units for Hive materialization. This source allows copying of tables,
 * materialization of views, and materialization of queries.
 */
public class HiveMaterializerSource implements Source<Object, Object> {

  private static final String HIVE_MATERIALIZER_SOURCE_PREFIX = "gobblin.hiveMaterializerSource";
  public static final String COPY_TABLE_KEY = HIVE_MATERIALIZER_SOURCE_PREFIX + ".copyTable";
  public static final String MATERIALIZE_VIEW = HIVE_MATERIALIZER_SOURCE_PREFIX + ".materializeView";
  public static final String MATERIALIZE_QUERY = HIVE_MATERIALIZER_SOURCE_PREFIX + ".materializeQuery";
  public static final String OUTPUT_STORAGE_FORMAT = HIVE_MATERIALIZER_SOURCE_PREFIX + ".outputStorageFormat";

  @Override
  public List<WorkUnit> getWorkunits(SourceState state) {
    try {
      FileSystem fs = HadoopUtils.getSourceFileSystem(state);
      Config config = ConfigUtils.propertiesToConfig(state.getProperties());

      if (state.contains(COPY_TABLE_KEY)) {
        HiveDataset dataset = getHiveDataset(state.getProp(COPY_TABLE_KEY), fs, state);
        WorkUnit workUnit = HiveMaterializer.tableCopyWorkUnit(dataset,
            new StageableTableMetadata(config.getConfig(HIVE_MATERIALIZER_SOURCE_PREFIX), dataset.getTable()), null);
        HiveTask.disableHiveWatermarker(workUnit);
        return Lists.newArrayList(workUnit);
      } else if (state.contains(MATERIALIZE_VIEW)) {
        HiveDataset dataset = getHiveDataset(state.getProp(MATERIALIZE_VIEW), fs, state);
        WorkUnit workUnit = HiveMaterializer.viewMaterializationWorkUnit(dataset, getOutputStorageFormat(state),
            new StageableTableMetadata(config.getConfig(HIVE_MATERIALIZER_SOURCE_PREFIX), dataset.getTable()), null);
        HiveTask.disableHiveWatermarker(workUnit);
        return Lists.newArrayList(workUnit);
      } else if (state.contains(MATERIALIZE_QUERY)) {
        String query = state.getProp(MATERIALIZE_QUERY);
        WorkUnit workUnit = HiveMaterializer.queryResultMaterializationWorkUnit(query, getOutputStorageFormat(state),
            new StageableTableMetadata(config.getConfig(HIVE_MATERIALIZER_SOURCE_PREFIX), null));
        HiveTask.disableHiveWatermarker(workUnit);
        return Lists.newArrayList(workUnit);
      }
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
    throw new RuntimeException(String.format("Must specify either %s, %s, or %s.", COPY_TABLE_KEY, MATERIALIZE_QUERY,
        MATERIALIZE_VIEW));
  }

  private HiveDataset getHiveDataset(String tableString, FileSystem fs, State state) throws IOException {
    try {
      HiveMetastoreClientPool pool = HiveMetastoreClientPool.get(state.getProperties(),
          Optional.fromNullable(state.getProp(HIVE_METASTORE_URI_KEY)));

      List<String> tokens = Splitter.on(".").splitToList(tableString);
      DbAndTable sourceDbAndTable = new DbAndTable(tokens.get(0), tokens.get(1));

      try (AutoReturnableObject<IMetaStoreClient> client = pool.getClient()) {
        Table sourceTable = new Table(client.get().getTable(sourceDbAndTable.getDb(), sourceDbAndTable.getTable()));
        return new HiveDataset(fs, pool, sourceTable, ConfigUtils.propertiesToConfig(state.getProperties()));
      }
    } catch (TException exc) {
      throw new RuntimeException(exc);
    }
  }

  private HiveConverterUtils.StorageFormat getOutputStorageFormat(State state) {
    return HiveConverterUtils.StorageFormat.valueOf(state.getProp(OUTPUT_STORAGE_FORMAT,
        HiveConverterUtils.StorageFormat.TEXT_FILE.name()));
  }

  @Override
  public Extractor<Object, Object> getExtractor(WorkUnitState state) throws IOException {
    return null;
  }

  @Override
  public void shutdown(SourceState state) {

  }
}
