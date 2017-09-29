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
import org.apache.gobblin.data.management.conversion.hive.task.HiveConverterUtils;
import org.apache.gobblin.data.management.copy.hive.HiveDatasetFinder;

import com.google.common.collect.Lists;


/**
 * A {@link org.apache.gobblin.data.management.conversion.hive.task.QueryGenerator} to materialize a copy of an existing
 * Hive table / partition.
 */
public class MaterializeTableQueryGenerator extends HiveMaterializerFromEntityQueryGenerator {

  private final HiveConverterUtils.StorageFormat storageFormat;

  public MaterializeTableQueryGenerator(WorkUnitState workUnitState) throws IOException {
    super(workUnitState, false);

    this.storageFormat = HiveConverterUtils.StorageFormat.valueOf(workUnitState.getProp(HiveMaterializer.STORAGE_FORMAT_KEY));
  }

  @Override
  public List<String> generateQueries() {
    ensureParentOfStagingPathExists();
    return Lists.newArrayList(HiveConverterUtils.generateStagingCTASStatementFromSelectStar(
        new HiveDatasetFinder.DbAndTable(this.outputDatabaseName, this.stagingTableName),
        new HiveDatasetFinder.DbAndTable(this.inputDbName, this.inputTableName),
        this.partitionsDMLInfo, this.storageFormat,
        this.stagingDataLocation));
  }
}
