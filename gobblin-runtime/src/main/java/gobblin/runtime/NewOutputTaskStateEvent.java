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

package gobblin.runtime;

/**
 * An event triggered when a new output {@link TaskState} is collected.
 *
 * @author ynli
 */
public class NewOutputTaskStateEvent {

  private final TaskState taskState;

  public NewOutputTaskStateEvent(TaskState taskState) {
    this.taskState = taskState;
  }

  /**
   * Get the {@link TaskState}.
   *
   * @return the {@link TaskState}
   */
  public TaskState getTaskState() {
    return this.taskState;
  }
}
