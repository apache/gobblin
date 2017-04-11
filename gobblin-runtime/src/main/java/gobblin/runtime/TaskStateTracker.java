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

package gobblin.runtime;

import com.google.common.util.concurrent.Service;


/**
 * An interface for classes that track {@link TaskState}s.
 *
 * @author Yinan Li
 */
public interface TaskStateTracker extends Service {

  /**
   * Register a new {@link Task}.
   *
   * @param task {@link Task} to register
   */
  public void registerNewTask(Task task);

  /**
   * Callback method when the {@link Task} completes running.
   *
   * @param task {@link Task} that runs completely.
   */
  public void onTaskRunCompletion(Task task);

  /**
   * Callback method when the {@link Task} completes committing.
   *
   * @param task {@link Task} that commits completely.
   */
  public void onTaskCommitCompletion(Task task);
}
