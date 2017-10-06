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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.DataConversionException;
import org.apache.gobblin.data.management.conversion.hive.entities.QueryBasedHivePublishEntity;
import org.apache.gobblin.data.management.conversion.hive.query.HiveAvroORCQueryGenerator;
import org.apache.gobblin.data.management.conversion.hive.task.HiveConverterUtils;
import org.apache.gobblin.data.management.copy.hive.HiveDatasetFinder;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;

import lombok.extern.slf4j.Slf4j;


/**
 * A {@link org.apache.gobblin.data.management.conversion.hive.task.QueryGenerator} to materialize the result of a Hive
 * query.
 */
@Slf4j
public class QueryBasedMaterializerQueryGenerator extends HiveMaterializerQueryGenerator {

  private final String sourceQuery;
  private final HiveConverterUtils.StorageFormat storageFormat;

  public QueryBasedMaterializerQueryGenerator(WorkUnitState workUnitState) throws IOException {
    super(workUnitState);

    this.sourceQuery = workUnitState.getProp(HiveMaterializer.QUERY_RESULT_TO_MATERIALIZE_KEY);
    this.storageFormat = HiveConverterUtils.StorageFormat.valueOf(workUnitState.getProp(HiveMaterializer.STORAGE_FORMAT_KEY));
  }

  @Override
  public List<String> generateQueries() {
    ensureParentOfStagingPathExists();
    return Lists.newArrayList(HiveConverterUtils.generateStagingCTASStatement(
        new HiveDatasetFinder.DbAndTable(this.outputDatabaseName, this.stagingTableName),
        this.sourceQuery,
        this.storageFormat,
        this.stagingDataLocation));
  }

  @Override
  public QueryBasedHivePublishEntity generatePublishQueries() throws DataConversionException {
    QueryBasedHivePublishEntity publishEntity = new QueryBasedHivePublishEntity();
    List<String> publishQueries = publishEntity.getPublishQueries();
    Map<String, String> publishDirectories = publishEntity.getPublishDirectories();
    List<String> cleanupQueries = publishEntity.getCleanupQueries();
    List<String> cleanupDirectories = publishEntity.getCleanupDirectories();

    String createFinalTableDDL =
        HiveConverterUtils.generateCreateDuplicateTableDDL(outputDatabaseName, stagingTableName, outputTableName,
            outputDataLocation, Optional.of(outputDatabaseName));
    publishQueries.add(createFinalTableDDL);
    log.debug("Create final table DDL:\n" + createFinalTableDDL);

    log.debug("Snapshot directory to move: " + stagingDataLocation + " to: " + outputDataLocation);
    publishDirectories.put(stagingDataLocation, outputDataLocation);

    String dropStagingTableDDL = HiveAvroORCQueryGenerator.generateDropTableDDL(outputDatabaseName, stagingTableName);

    log.debug("Drop staging table DDL: " + dropStagingTableDDL);
    cleanupQueries.add(dropStagingTableDDL);

    log.debug("Staging table directory to delete: " + stagingDataLocation);
    cleanupDirectories.add(stagingDataLocation);


    publishQueries.addAll(HiveAvroORCQueryGenerator.generateDropPartitionsDDL(outputDatabaseName, outputTableName,
        new HashMap<>()));

    log.info("Publish partition entity: " + publishEntity);
    return publishEntity;
  }
}
