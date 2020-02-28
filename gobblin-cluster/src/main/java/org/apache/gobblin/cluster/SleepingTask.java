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

import java.io.File;
import java.io.IOException;

import org.apache.gobblin.runtime.TaskContext;
import org.apache.gobblin.runtime.task.BaseAbstractTask;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.io.Files;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SleepingTask extends BaseAbstractTask {
  public static final String TASK_STATE_FILE_KEY = "task.state.file.path";
  public static final String SLEEPING_TASK_SLEEP_TIME = "data.publisher.sleep.time.in.seconds";
  public static final String SLEEPING_TASK_SUICIDE_TIME = "timeBeforeSuicideAfterStart.in.seconds";

  private final long sleepTime;
  private final long suicideTime;
  private File taskStateFile;
  private final TaskContext taskContext;

  public SleepingTask(TaskContext taskContext) {
    super(taskContext);
    this.taskContext = taskContext;
    sleepTime = taskContext.getTaskState().getPropAsLong(SLEEPING_TASK_SLEEP_TIME, 10L);
    suicideTime = taskContext.getTaskState().getPropAsLong(SLEEPING_TASK_SUICIDE_TIME, 2L);

    // We need to suicide the task before sleeping finish if we configure the task to commit suicide.
    Preconditions.checkArgument(suicideTime < sleepTime);
    taskStateFile = new File(taskContext.getTaskState().getProp(TASK_STATE_FILE_KEY));
    try {
      if (taskStateFile.exists()) {
        if (!taskStateFile.delete()) {
          log.error("Unable to delete {}", taskStateFile);
          throw new IOException("File Delete Exception");
        }
      } else {
        Files.createParentDirs(taskStateFile);
      }
    } catch (IOException e) {
      log.error("Unable to create directory: ", taskStateFile.getParent());
      Throwables.propagate(e);
    }
    taskStateFile.deleteOnExit();
  }

  @Override
  public void run() {
    try {
      if (!taskStateFile.createNewFile()) {
        throw new IOException("File creation error: " + taskStateFile.getName());
      }
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
      failTask(e, taskContext);
    } catch (IOException e) {
      log.error("IOException encountered when creating {}", taskStateFile.getName(), e);
      failTask(e, taskContext);
    }
  }
}
