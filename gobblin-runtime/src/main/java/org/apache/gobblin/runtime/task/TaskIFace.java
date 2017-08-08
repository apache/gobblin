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

package org.apache.gobblin.runtime.task;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;


/**
 * The interface that a task to be run in Gobblin should implement.
 */
@Alpha
public interface TaskIFace extends Runnable {

  /**
   * Execute the bulk of the work for this task. Due to possible speculative execution, all operations in this method
   * should be attempt-isolated.
   */
  void run();

  /**
   * Commit the data to the job staging location. This method is guaranteed to be called only once per task, even if
   * a task is attempted in multiple containers.
   */
  void commit();

  /**
   * @return The persistent state for this task. This is the state that will be available in the next Gobblin execution.
   */
  State getPersistentState();

  /**
   * @return The metadata corresponding to this task execution. This metadata will be available to the publisher and may
   *         be viewable by the user, but may not be stored for the next Gobblin execution.
   */
  State getExecutionMetadata();

  /**
   * @return The current working state of the task.
   */
  WorkUnitState.WorkingState getWorkingState();

  /**
   * Prematurely shutdown the task.
   */
  void shutdown();

  /**
   * Block until the task finishes shutting down. This method is guaranteed to only be called after {@link #shutdown()}
   * is called.
   * @param timeoutMillis The method should return after this many millis regardless of the shutdown of the task.
   * @return true if the task shut down correctly.
   * @throws InterruptedException
   */
  boolean awaitShutdown(long timeoutMillis) throws InterruptedException;

  /**
   * @return a string describing the progress of this task.
   */
  String getProgress();

  /**
   * @return true if the {@link #run()} method is safe to be executed in various executors at the same time.
   */
  boolean isSpeculativeExecutionSafe();

}
