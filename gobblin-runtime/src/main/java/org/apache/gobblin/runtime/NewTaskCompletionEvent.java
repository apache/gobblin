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

package org.apache.gobblin.runtime;

import java.util.Collection;


/**
 * An event triggered upon the completion of one or more {@link Task}s.
 *
 * <p>
 *   This event carries the {@link TaskState}(s) of the completed {@link Task}(s). Classes that are
 *   interested in receiving the events can registered themselves to the
 *   {@link com.google.common.eventbus.EventBus} in {@link AbstractJobLauncher} to which the events
 *   are posted.
 * </p>
 *
 * @author Yinan Li
 */
public class NewTaskCompletionEvent {

  private final Collection<TaskState> taskStates;

  public NewTaskCompletionEvent(Collection<TaskState> taskStates) {
    this.taskStates = taskStates;
  }

  /**
   * Get the {@link Collection} of {@link TaskState}s of completed {@link Task}s.
   *
   * @return the {@link Collection} of {@link TaskState}s of completed {@link Task}s
   */
  public Collection<TaskState> getTaskStates() {
    return this.taskStates;
  }
}
