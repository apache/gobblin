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
import org.apache.gobblin.data.management.conversion.hive.events.EventWorkunitUtils;
import org.apache.gobblin.data.management.conversion.hive.task.HiveConverterUtils;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import lombok.extern.slf4j.Slf4j;


/**
 * A {@link org.apache.gobblin.data.management.conversion.hive.task.QueryGenerator} that generates queries to exactly
 * copy an input table / partition.
 */
@Slf4j
public class CopyTableQueryGenerator extends HiveMaterializerFromEntityQueryGenerator {

  public CopyTableQueryGenerator(WorkUnitState workUnitState) throws IOException {
    super(workUnitState, true);
  }

  /**
   * Returns hive queries to be run as a part of a hive task.
   * This does not include publish queries.
   * @return
   */
  @Override
  public List<String> generateQueries() {

    ensureParentOfStagingPathExists();

    List<String> hiveQueries = Lists.newArrayList();
    /*
     * Setting partition mode to 'nonstrict' is needed to improve readability of the code.
     * If we do not set dynamic partition mode to nonstrict, we will have to write partition values also,
     * and because hive considers partition as a virtual column, we also have to write each of the column
     * name in the query (in place of *) to match source and target columns.
     */
    hiveQueries.add("SET hive.exec.dynamic.partition.mode=nonstrict");

    Preconditions.checkNotNull(this.workUnit, "Workunit must not be null");
    EventWorkunitUtils.setBeginDDLBuildTimeMetadata(this.workUnit, System.currentTimeMillis());

    HiveConverterUtils.createStagingDirectory(fs, outputTableMetadata.getDestinationDataPath(),
        conversionEntity, this.workUnitState);

    // Create DDL statement for table
    String createStagingTableDDL =
        HiveConverterUtils.generateCreateDuplicateTableDDL(
            inputDbName,
            inputTableName,
            stagingTableName,
            stagingDataLocation,
            Optional.of(outputDatabaseName));
    hiveQueries.add(createStagingTableDDL);
    log.debug("Create staging table DDL:\n" + createStagingTableDDL);


    String insertInStagingTableDML =
        HiveConverterUtils
            .generateTableCopy(
                inputTableName,
                stagingTableName,
                conversionEntity.getTable().getDbName(),
                outputDatabaseName,
                Optional.of(partitionsDMLInfo));
    hiveQueries.add(insertInStagingTableDML);
    log.debug("Conversion staging DML: " + insertInStagingTableDML);

    log.info("Conversion Queries {}\n",  hiveQueries);

    EventWorkunitUtils.setEndDDLBuildTimeMetadata(workUnit, System.currentTimeMillis());
    return hiveQueries;
  }

}
