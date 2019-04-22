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

package org.apache.gobblin.cluster;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import avro.shaded.com.google.common.base.Throwables;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.runtime.TaskContext;
import org.apache.gobblin.runtime.TaskState;
import org.apache.gobblin.runtime.task.BaseAbstractTask;

@Slf4j
public class SleepingTask extends BaseAbstractTask {
  public static final String TASK_STATE_FILE_KEY = "task.state.file.path";

  private final long sleepTime;
  private FileSystem fs;
  private Path taskStateFile;

  public SleepingTask(TaskContext taskContext) {
    super(taskContext);
    TaskState taskState = taskContext.getTaskState();
    sleepTime = taskState.getPropAsLong("data.publisher.sleep.time.in.seconds", 10L);
    taskStateFile = new Path(taskState.getProp(TASK_STATE_FILE_KEY));
    try {
      fs = FileSystem.getLocal(new Configuration());
      //Delete any previous left over task state files
      if (this.fs.exists(taskStateFile)) {
        this.fs.delete(taskStateFile, false);
      }
    } catch (IOException e) {
      log.error("Error creating SleepingTask.");
      Throwables.propagate(e);
    }
  }

  @Override
  public void run() {
    try {
      if (!this.fs.exists(taskStateFile.getParent())) {
        this.fs.mkdirs(taskStateFile.getParent());
      }
      this.fs.create(taskStateFile);
      long endTime = System.currentTimeMillis() + sleepTime * 1000;
      while (System.currentTimeMillis() <= endTime) {
        Thread.sleep(1000L);
        log.warn("Sleeping for {} seconds", sleepTime);
      }
      log.info("Hello World!");
      super.run();
    } catch (InterruptedException e) {
      log.error("Sleep interrupted.");
      Thread.currentThread().interrupt();
      Throwables.propagate(e);
    } catch (IOException e) {
      log.error("IOException encountered", e);
      Throwables.propagate(e);
    } finally {
      try {
        if (this.fs.exists(taskStateFile)) {
          this.fs.delete(taskStateFile, false);
        }
      } catch (IOException e) {
        log.warn("Error when attempting to delete {}", taskStateFile, e);
      }
    }
  }
}
