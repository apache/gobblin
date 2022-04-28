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

package org.apache.gobblin.runtime.retention;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.data.management.retention.DatasetCleaner;
import org.apache.gobblin.runtime.TaskContext;
import org.apache.gobblin.runtime.task.BaseAbstractTask;
import org.apache.gobblin.util.JobConfigurationUtils;


/**
 * A task that runs a DatasetCleaner job.
 */
@Slf4j
public class DatasetCleanerTask extends BaseAbstractTask {

  private static final String JOB_CONFIGURATION_PREFIX = "datasetCleaner";

  private final TaskContext taskContext;

  public DatasetCleanerTask(TaskContext taskContext) {
    super(taskContext);
    this.taskContext = taskContext;
  }

  @Override
  public void run(){
    DatasetCleaner datasetCleaner = null;
    try {
      Configuration conf = new Configuration();
      JobConfigurationUtils.putStateIntoConfiguration(this.taskContext.getTaskState(), conf);
      datasetCleaner = new DatasetCleaner(FileSystem.get(conf), this.taskContext.getTaskState().getProperties());
      datasetCleaner.clean();
      this.workingState = WorkUnitState.WorkingState.SUCCESSFUL;
    } catch (IOException e) {
      this.workingState = WorkUnitState.WorkingState.FAILED;
      throw new RuntimeException(e);
    } finally {
      if (datasetCleaner != null) {
        try {
          datasetCleaner.close();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  @Override
  public void commit() {
    log.info("task {} commits with state {}", this.taskContext.getTaskState().getTaskId(), this.workingState);
  }
}
