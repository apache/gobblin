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

package org.apache.gobblin.data.management.conversion.hive.task;

import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.data.management.conversion.hive.dataset.ConvertibleHiveDataset;
import org.apache.gobblin.data.management.conversion.hive.entities.QueryBasedHivePublishEntity;
import org.apache.gobblin.data.management.conversion.hive.watermarker.PartitionLevelWatermarker;
import org.apache.gobblin.runtime.TaskContext;


@Slf4j
public class HiveMaterializer extends HiveTask {

  public HiveMaterializer(TaskContext taskContext) throws Exception {
    super(taskContext);
    if (Boolean.valueOf(this.workUnitState.getPropAsBoolean(PartitionLevelWatermarker.IS_WATERMARK_WORKUNIT_KEY))) {
      log.info("HiveMaterializer, Ignoring Watermark workunit for {}", this.workUnitState.getProp(ConfigurationKeys.DATASET_URN_KEY));
      return;
    }
    if (!(workUnit.getHiveDataset() instanceof ConvertibleHiveDataset)) {
      throw new IllegalStateException("HiveConvertExtractor is only compatible with ConvertibleHiveDataset");
    }
    this.queryGenerator = new HiveMaterializerQueryGenerator(this.workUnitState);
  }

  @Override
  public List<String> generateHiveQueries() {
    return queryGenerator.generateQueries();
  }

  @Override
  public QueryBasedHivePublishEntity generatePublishQueries() throws Exception {
    return queryGenerator.generatePublishQueries();
  }

  @Override
  public void run() {
    try {
      this.hiveExecutionQueries = generateHiveQueries();
      executeQueries(this.hiveExecutionQueries);
    } catch (Exception e) {
      log.error("Exception in HiveMaterializer generate/execute HiveQueries ", e);
    }
  }

  @Override
  public void commit() {
    try {
      this.queryBasedHivePublishEntity = generatePublishQueries();
      executePublishQueries(publishEntity);
    } catch (Exception e) {
      log.error("Exception in HiveMaterializer generate/execute publish queries ", e);
    }
    super.commit();
  }
}