/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.yarn;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.helix.task.Task;
import org.apache.helix.task.TaskCallbackContext;
import org.apache.helix.task.TaskFactory;

import gobblin.runtime.TaskExecutor;
import gobblin.runtime.TaskStateTracker;


/**
 * An implementation of Helix's {@link TaskFactory} for {@link GobblinHelixTask}s.
 *
 * @author ynli
 */
public class GobblinHelixTaskFactory implements TaskFactory {

  private final TaskExecutor taskExecutor;
  private final TaskStateTracker taskStateTracker;
  private final FileSystem fs;
  private final Path appWorkDir;

  public GobblinHelixTaskFactory(TaskExecutor taskExecutor, TaskStateTracker taskStateTracker,
      FileSystem fs, Path appWorkDir) {
    this.taskExecutor = taskExecutor;
    this.taskStateTracker = taskStateTracker;
    this.fs = fs;
    this.appWorkDir = appWorkDir;
  }

  @Override
  public Task createNewTask(TaskCallbackContext context) {
    try {
      return new GobblinHelixTask(context, this.taskExecutor, this.taskStateTracker, this.fs, this.appWorkDir);
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to create a new GobblinHelixTask", ioe);
    }
  }
}
