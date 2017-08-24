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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.data.management.conversion.hive.entities.QueryBasedHivePublishEntity;
import org.apache.gobblin.data.management.conversion.hive.source.HiveSource;
import org.apache.gobblin.data.management.conversion.hive.source.HiveWorkUnit;
import org.apache.gobblin.data.management.conversion.hive.watermarker.HiveSourceWatermarker;
import org.apache.gobblin.data.management.conversion.hive.watermarker.HiveSourceWatermarkerFactory;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.runtime.TaskContext;
import org.apache.gobblin.runtime.task.BaseAbstractTask;
import org.apache.gobblin.util.HiveJdbcConnector;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;
import org.apache.hadoop.fs.FileSystem;


@Slf4j
public class HiveTask extends BaseAbstractTask {
  protected final TaskContext taskContext;
  protected final WorkUnitState workUnitState;
  protected final HiveWorkUnit workUnit;
  protected final EventSubmitter eventSubmitter;
  protected List<String> hiveExecutionQueries;
  protected QueryBasedHivePublishEntity queryBasedHivePublishEntity;
  protected QueryGenerator queryGenerator;
  protected HiveJdbcConnector hiveJdbcConnector;
  protected final QueryBasedHivePublishEntity publishEntity;

  public HiveTask(TaskContext taskContext) {
    super(taskContext);
    this.taskContext = taskContext;
    this.workUnitState = taskContext.getTaskState();
    this.workUnit = new HiveWorkUnit(this.workUnitState.getWorkunit());
    this.eventSubmitter = new EventSubmitter.Builder(this.metricContext, "gobblin.HiveTask")
        .build();
    this.hiveExecutionQueries = Lists.newArrayList();
    try {
      this.hiveJdbcConnector = HiveJdbcConnector.newConnectorWithProps(this.workUnitState.getProperties());
    } catch (SQLException se) {
      log.error("Error in creating JDBC Connector", se);
    }
    this.queryBasedHivePublishEntity = new QueryBasedHivePublishEntity();
    this.publishEntity = new QueryBasedHivePublishEntity();
  }

  /**
   * Generate hive queries to extract data
   * @return list of hive queries
   * @throws Exception
   */
  public List<String> generateHiveQueries() throws Exception {
    return Lists.newArrayList();
  }

  /**
   * Generate publish and cleanup queries for hive datasets/partitions
   * @return QueryBasedHivePublishEntity having cleanup and publish queries
   * @throws Exception
   */
  public QueryBasedHivePublishEntity generatePublishQueries() throws Exception {
    return new QueryBasedHivePublishEntity();
  }

  protected void executeQueries(List<String> queries) {
    if (null == queries || queries.size() == 0) {
      return;
    }
    try {
      this.hiveJdbcConnector.executeStatements(queries.toArray(new String[queries.size()]));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  protected void executePublishQueries(QueryBasedHivePublishEntity publishEntity) {
    Set<String> cleanUpQueries = Sets.newLinkedHashSet();
    Set<String> publishQueries = Sets.newLinkedHashSet();
    List<String> directoriesToDelete = Lists.newArrayList();
    FileSystem fs = null;

    try {
      fs = HiveConverterUtils.getSourceFs(workUnitState);

      if (publishEntity.getCleanupQueries() != null) {
        cleanUpQueries.addAll(publishEntity.getCleanupQueries());
      }

      if (publishEntity.getCleanupDirectories() != null) {
        directoriesToDelete.addAll(publishEntity.getCleanupDirectories());
      }

      if (publishEntity.getPublishDirectories() != null) {
        // Publish snapshot / partition directories
        Map<String, String> publishDirectories = publishEntity.getPublishDirectories();
        try {
          for (Map.Entry<String, String> publishDir : publishDirectories.entrySet()) {
            HiveConverterUtils.moveDirectory(fs, publishDir.getKey(), publishDir.getValue());
          }
        } catch (Exception e) {
          log.error("error in move dir");
        }
      }

      if (publishEntity.getPublishQueries() != null) {
        publishQueries.addAll(publishEntity.getPublishQueries());
      }

      WorkUnitState wus = this.workUnitState;

      // Actual publish: Register snapshot / partition
      executeQueries(Lists.newArrayList(publishQueries));

      wus.setWorkingState(WorkUnitState.WorkingState.COMMITTED);

      HiveSourceWatermarker watermarker = GobblinConstructorUtils.invokeConstructor(
          HiveSourceWatermarkerFactory.class, wus.getProp(HiveSource.HIVE_SOURCE_WATERMARKER_FACTORY_CLASS_KEY,
              HiveSource.DEFAULT_HIVE_SOURCE_WATERMARKER_FACTORY_CLASS)).createFromState(wus);

      watermarker.setActualHighWatermark(wus);
    } catch (Exception e) {
      log.error("Error in HiveMaterializer generate publish queries", e);
    } finally {

      try {
        executeQueries(Lists.newArrayList(cleanUpQueries));
      } catch (Exception e) {
        log.error("Failed to cleanup staging entities in Hive metastore.", e);
      }
      try {
        HiveConverterUtils.deleteDirectories(fs, directoriesToDelete);
      } catch (Exception e) {
        log.error("Failed to cleanup staging directories.", e);
      }
    }
  }

  @Override
  public void run() {
    try {
      executeQueries(generateHiveQueries());
    } catch (Exception e) {
      log.error("Exception in HiveTask generateHiveQueries ", e);
    }
    super.run();
  }

  @Override
  public void commit() {
    try {
      executePublishQueries(generatePublishQueries());
    } catch (Exception e) {
      log.error("Exception in HiveTask generate publish HiveQueries ", e);
    }
    super.commit();
  }

}
