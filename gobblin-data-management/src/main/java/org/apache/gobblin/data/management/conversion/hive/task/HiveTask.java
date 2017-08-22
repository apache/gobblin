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
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.data.management.conversion.hive.entities.QueryBasedHivePublishEntity;
import org.apache.gobblin.data.management.conversion.hive.source.HiveWorkUnit;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.runtime.TaskContext;
import org.apache.gobblin.runtime.task.BaseAbstractTask;
import org.apache.gobblin.util.HiveJdbcConnector;


@Slf4j
public abstract class HiveTask extends BaseAbstractTask {
  protected final TaskContext taskContext;
  protected final WorkUnitState workUnitState;
  protected final HiveWorkUnit workUnit;
  private final EventSubmitter eventSubmitter;
  private final List<String> hiveExecutionQueries;
  QueryBasedHivePublishEntity queryBasedHivePublishEntity;
  private HiveQueryGenerator queryGenerator;
  public HiveJdbcConnector hiveJdbcConnector;
  protected QueryBasedHivePublishEntity publishEntity;

  public HiveTask(TaskContext taskContext) {
    super(taskContext);
    this.taskContext = taskContext;
    this.workUnitState = taskContext.getTaskState();
    this.workUnit = new HiveWorkUnit(this.workUnitState.getWorkunit());
    this.eventSubmitter = new EventSubmitter.Builder(this.metricContext, "gobblin.HiveTask")
        .build();
    this.hiveExecutionQueries = Lists.newArrayList();
    this.queryBasedHivePublishEntity = new QueryBasedHivePublishEntity();
    this.publishEntity = new QueryBasedHivePublishEntity();
  }

  // Should populate this.hiveExecutionQueries with Hive queries to be executed as part of run
  // this.taskContext should have all the information needed to create queries.
  public abstract void generateHiveQueries() throws Exception;

  public void generatePublishQueries() throws Exception {
    // Should generate and store publish queries like file/directory move/delete into this.queryBasedHivePublishEntity
  }

  @Override
  public void run() {
    try {
      generateHiveQueries();
    } catch (Exception e) {
      log.error("Exception in HiveTask generateHiveQueries ", e);
    }
    super.run();
    // Should run queries in this.hiveExecutionQueries
  }

  @Override
  public void commit() {
    // Should run queries in this.queryBasedHivePublishEntity
    try {
      generatePublishQueries();
    } catch (Exception e) {
      log.error("Exception in HiveTask generate publish HiveQueries ", e);
    }
    super.commit();
  }

}
