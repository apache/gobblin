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

import org.apache.gobblin.data.management.conversion.hive.entities.StageableTableMetadata;
import org.apache.gobblin.data.management.conversion.hive.source.HiveSource;
import org.apache.gobblin.data.management.conversion.hive.task.HiveConverterUtils;
import org.apache.gobblin.data.management.conversion.hive.task.QueryGenerator;
import org.apache.hadoop.fs.FileSystem;

import com.google.common.base.Optional;

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.DataConversionException;
import org.apache.gobblin.data.management.conversion.hive.entities.QueryBasedHivePublishEntity;
import org.apache.gobblin.data.management.conversion.hive.source.HiveWorkUnit;
import org.apache.gobblin.data.management.copy.hive.HiveDatasetFinder;
import org.apache.gobblin.hive.HiveMetastoreClientPool;
import org.apache.hadoop.fs.Path;

import lombok.extern.slf4j.Slf4j;

@Slf4j
/**
 * A base abstract query generator for {@link HiveMaterializer}.
 */
public abstract class HiveMaterializerQueryGenerator implements QueryGenerator {
  protected final FileSystem fs;
  protected final StageableTableMetadata outputTableMetadata;

  protected final String outputDatabaseName;
  protected final String outputTableName;
  protected final String outputDataLocation;

  protected final String stagingTableName;
  protected final String stagingDataLocation;

  protected final Optional<org.apache.hadoop.hive.metastore.api.Table> destinationTableMeta;
  protected final HiveWorkUnit workUnit;
  protected final HiveMetastoreClientPool pool;
  protected final WorkUnitState workUnitState;

  public HiveMaterializerQueryGenerator(WorkUnitState workUnitState) throws IOException {
    this.fs = HiveSource.getSourceFs(workUnitState);
    this.pool = HiveMetastoreClientPool.get(workUnitState.getJobState().getProperties(),
        Optional.fromNullable(workUnitState.getJobState().getProp(HiveDatasetFinder.HIVE_METASTORE_URI_KEY)));

    this.workUnitState = workUnitState;
    this.workUnit = new HiveWorkUnit(workUnitState.getWorkunit());

    this.outputTableMetadata = HiveMaterializer.parseStageableTableMetadata(this.workUnit);
    this.outputDatabaseName = outputTableMetadata.getDestinationDbName();
    this.outputTableName = outputTableMetadata.getDestinationTableName();
    this.outputDataLocation = HiveConverterUtils.getOutputDataLocation(outputTableMetadata.getDestinationDataPath());

    this.destinationTableMeta = HiveConverterUtils.getDestinationTableMeta(this.outputTableMetadata.getDestinationDbName(),
        this.outputTableMetadata.getDestinationTableName(), workUnitState.getProperties()).getLeft();

    this.stagingTableName = HiveConverterUtils.getStagingTableName(this.outputTableMetadata.getDestinationStagingTableName());
    this.stagingDataLocation = HiveConverterUtils.getStagingDataLocation(this.outputTableMetadata.getDestinationDataPath(), this.stagingTableName);
  }

  /**
   * Returns hive queries to be run as a part of a hive task.
   * This does not include publish queries.
   */
  @Override
  public abstract List<String> generateQueries();

  protected void ensureParentOfStagingPathExists() {
    try {
      Path parentStagingPath = new Path(this.stagingDataLocation).getParent();
      if (!this.fs.exists(parentStagingPath)) {
        this.fs.mkdirs(parentStagingPath);
      }
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  /**
   * Retuens a QueryBasedHivePublishEntity which includes publish level queries and cleanup commands.
   * @return QueryBasedHivePublishEntity
   * @throws DataConversionException
   */
  public abstract QueryBasedHivePublishEntity generatePublishQueries() throws DataConversionException;
}
