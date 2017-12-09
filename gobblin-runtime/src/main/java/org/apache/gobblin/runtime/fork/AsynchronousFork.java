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

package org.apache.gobblin.runtime.fork;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.gobblin.runtime.BoundedBlockingRecordQueue;
import org.apache.gobblin.runtime.ExecutionModel;
import org.apache.gobblin.runtime.Task;
import org.apache.gobblin.runtime.TaskContext;
import org.apache.gobblin.runtime.TaskExecutor;
import org.apache.gobblin.runtime.TaskState;
import lombok.extern.slf4j.Slf4j;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.converter.DataConversionException;


/**
 * A class representing a forked branch of operations of a {@link Task} flow. The {@link Fork}s of a
 * {@link Task} are executed in a thread pool managed by the {@link TaskExecutor}, which is different
 * from the thread pool used to execute {@link Task}s.
 *
 * <p>
 *     Each {@link Fork} consists of the following steps:
 *     <ul>
 *       <li>Getting the next record off the record queue.</li>
 *       <li>Converting the record and doing row-level quality checking if applicable.</li>
 *       <li>Writing the record out if it passes the quality checking.</li>
 *       <li>Cleaning up and exiting once all the records have been processed.</li>
 *     </ul>
 * </p>
 *
 * @author Yinan Li
 */
@Slf4j
@SuppressWarnings("unchecked")
public class AsynchronousFork extends Fork {
  private final BoundedBlockingRecordQueue<Object> recordQueue;

  public AsynchronousFork(TaskContext taskContext, Object schema, int branches, int index, ExecutionModel executionModel)
      throws Exception {
    super(taskContext, schema, branches, index, executionModel);
    TaskState taskState = taskContext.getTaskState();

    this.recordQueue = BoundedBlockingRecordQueue.newBuilder()
            .hasCapacity(taskState.getPropAsInt(
                    ConfigurationKeys.FORK_RECORD_QUEUE_CAPACITY_KEY,
                    ConfigurationKeys.DEFAULT_FORK_RECORD_QUEUE_CAPACITY))
            .useTimeout(taskState.getPropAsLong(
                    ConfigurationKeys.FORK_RECORD_QUEUE_TIMEOUT_KEY,
                    ConfigurationKeys.DEFAULT_FORK_RECORD_QUEUE_TIMEOUT))
            .useTimeoutTimeUnit(TimeUnit.valueOf(taskState.getProp(
                    ConfigurationKeys.FORK_RECORD_QUEUE_TIMEOUT_UNIT_KEY,
                    ConfigurationKeys.DEFAULT_FORK_RECORD_QUEUE_TIMEOUT_UNIT)))
            .collectStats()
            .build();
  }

  @Override
  public Optional<BoundedBlockingRecordQueue<Object>.QueueStats> queueStats() {
    return this.recordQueue.stats();
  }

  @Override
  protected void processRecords() throws IOException, DataConversionException {
    while (processRecord()) { }
  }

  @Override
  protected boolean putRecordImpl(Object record) throws InterruptedException {
    return this.recordQueue.put(record);
  }

  boolean processRecord() throws IOException, DataConversionException {
    try {
      Object record = this.recordQueue.get();
      if (record == null || record == Fork.SHUTDOWN_RECORD) {
        // The parent task has already done pulling records so no new record means this fork is done
        if (this.isParentTaskDone()) {
          return false;
        }
      } else {
        this.processRecord(record);
      }
    } catch (InterruptedException ie) {
      log.warn("Interrupted while trying to get a record off the queue", ie);
      Throwables.propagate(ie);
    }
    return true;
  }
}
