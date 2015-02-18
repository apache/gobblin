/* (c) 2014 LinkedIn Corp. All rights reserved.
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

import com.google.common.util.concurrent.Service;


/**
 * An interface for classes that track {@link TaskState}s.
 *
 * @author ynli
 */
public interface TaskStateTracker extends Service {

  /**
   * Register a new {@link Task}.
   *
   * @param task {@link Task} to register
   */
  public void registerNewTask(Task task);

  /**
   * Callback method when the {@link Task} is completed.
   *
   * @param task {@link Task} that is completed
   */
  public void onTaskCompletion(Task task);
}
