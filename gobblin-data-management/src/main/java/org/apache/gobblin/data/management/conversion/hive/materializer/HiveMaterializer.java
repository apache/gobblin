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

package org.apache.gobblin.data.management.conversion.hive.materializer;

import java.io.IOException;
import java.util.List;

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.data.management.conversion.hive.entities.QueryBasedHivePublishEntity;
import org.apache.gobblin.data.management.conversion.hive.entities.StageableTableMetadata;
import org.apache.gobblin.data.management.conversion.hive.source.HiveSource;
import org.apache.gobblin.data.management.conversion.hive.source.HiveWorkUnit;
import org.apache.gobblin.data.management.conversion.hive.task.HiveConverterUtils;
import org.apache.gobblin.data.management.conversion.hive.task.HiveTask;
import org.apache.gobblin.data.management.conversion.hive.task.QueryGenerator;
import org.apache.gobblin.data.management.copy.hive.HiveDataset;
import org.apache.gobblin.runtime.TaskContext;
import org.apache.gobblin.runtime.task.TaskUtils;
import org.apache.gobblin.source.workunit.WorkUnit;

import com.google.common.base.Strings;

import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j

/**
 * A simple {@link HiveTask} for Hive view materialization.
 */
public class HiveMaterializer extends HiveTask {

  protected static final String STAGEABLE_TABLE_METADATA_KEY = "internal.hiveMaterializer.stageableTableMetadata";
  protected static final String MATERIALIZER_MODE_KEY = "internal.hiveMaterializer.materializerMode";
  protected static final String STORAGE_FORMAT_KEY = "internal.hiveMaterializer.storageFormat";
  protected static final String QUERY_RESULT_TO_MATERIALIZE_KEY = "internal.hiveMaterializer.queryResultToMaterialize";

  /**
   * Create a work unit to copy a source table to a target table using a staging table in between.
   * @param dataset {@link HiveDataset} for the source table.
   * @param destinationTable {@link StageableTableMetadata} specifying staging and target tables metadata.
   */
  public static HiveWorkUnit tableCopyWorkUnit(HiveDataset dataset, StageableTableMetadata destinationTable,
      @Nullable String partitionName) {
    HiveWorkUnit workUnit = new HiveWorkUnit(dataset);
    workUnit.setProp(MATERIALIZER_MODE_KEY, MaterializerMode.TABLE_COPY.name());
    workUnit.setProp(STAGEABLE_TABLE_METADATA_KEY, HiveSource.GENERICS_AWARE_GSON.toJson(destinationTable));
    if (!Strings.isNullOrEmpty(partitionName)) {
      workUnit.setPartitionName(partitionName);
    }
    TaskUtils.setTaskFactoryClass(workUnit, HiveMaterializerTaskFactory.class);
    return workUnit;
  }

  /**
   * Create a work unit to materialize a table / view to a target table using a staging table in between.
   * @param dataset {@link HiveDataset} for the source table.
   * @param storageFormat format in which target table should be written.
   * @param destinationTable {@link StageableTableMetadata} specifying staging and target tables metadata.
   */
  public static HiveWorkUnit viewMaterializationWorkUnit(HiveDataset dataset, HiveConverterUtils.StorageFormat storageFormat,
      StageableTableMetadata destinationTable, @Nullable String partitionName) {
    HiveWorkUnit workUnit = new HiveWorkUnit(dataset);
    workUnit.setProp(MATERIALIZER_MODE_KEY, MaterializerMode.TABLE_MATERIALIZATION.name());
    workUnit.setProp(STORAGE_FORMAT_KEY, storageFormat.name());
    workUnit.setProp(STAGEABLE_TABLE_METADATA_KEY, HiveSource.GENERICS_AWARE_GSON.toJson(destinationTable));
    if (!Strings.isNullOrEmpty(partitionName)) {
      workUnit.setPartitionName(partitionName);
    }
    TaskUtils.setTaskFactoryClass(workUnit, HiveMaterializerTaskFactory.class);
    return workUnit;
  }

  /**
   * Create a work unit to materialize a query to a target table using a staging table in between.
   * @param query the query to materialize.
   * @param storageFormat format in which target table should be written.
   * @param destinationTable {@link StageableTableMetadata} specifying staging and target tables metadata.
   */
  public static WorkUnit queryResultMaterializationWorkUnit(String query, HiveConverterUtils.StorageFormat storageFormat,
      StageableTableMetadata destinationTable) {
    WorkUnit workUnit = new WorkUnit();
    workUnit.setProp(MATERIALIZER_MODE_KEY, MaterializerMode.QUERY_RESULT_MATERIALIZATION.name());
    workUnit.setProp(STORAGE_FORMAT_KEY, storageFormat.name());
    workUnit.setProp(QUERY_RESULT_TO_MATERIALIZE_KEY, query);
    workUnit.setProp(STAGEABLE_TABLE_METADATA_KEY, HiveSource.GENERICS_AWARE_GSON.toJson(destinationTable));
    TaskUtils.setTaskFactoryClass(workUnit, HiveMaterializerTaskFactory.class);
    HiveTask.disableHiveWatermarker(workUnit);
    return workUnit;
  }

  public static StageableTableMetadata parseStageableTableMetadata(WorkUnit workUnit) {
    return HiveSource.GENERICS_AWARE_GSON.fromJson(workUnit.getProp(STAGEABLE_TABLE_METADATA_KEY), StageableTableMetadata.class);
  }

  private enum MaterializerMode {

    /** Materialize a table or view into a new table possibly with a new storage format. */
    TABLE_MATERIALIZATION {
      @Override
      public QueryGenerator createQueryGenerator(WorkUnitState state) throws IOException {
        return new MaterializeTableQueryGenerator(state);
      }
    },
    /** Copy a table into a new table with the same properties. */
    TABLE_COPY {
      @Override
      public QueryGenerator createQueryGenerator(WorkUnitState state) throws IOException {
        return new CopyTableQueryGenerator(state);
      }
    },
    /** Materialize a query into a table. */
    QUERY_RESULT_MATERIALIZATION {
      @Override
      public QueryGenerator createQueryGenerator(WorkUnitState state) throws IOException {
        return new QueryBasedMaterializerQueryGenerator(state);
      }
    };

    public abstract QueryGenerator createQueryGenerator(WorkUnitState state) throws IOException;
  }

  private final QueryGenerator queryGenerator;

  public HiveMaterializer(TaskContext taskContext) throws IOException {
    super(taskContext);
    MaterializerMode materializerMode = MaterializerMode.valueOf(this.workUnitState.getProp(MATERIALIZER_MODE_KEY));
    this.queryGenerator = materializerMode.createQueryGenerator(this.workUnitState);
  }

  @Override
  public List<String> generateHiveQueries() {
    return queryGenerator.generateQueries();
  }

  @Override
  public QueryBasedHivePublishEntity generatePublishQueries() throws Exception {
    return queryGenerator.generatePublishQueries();
  }
}
