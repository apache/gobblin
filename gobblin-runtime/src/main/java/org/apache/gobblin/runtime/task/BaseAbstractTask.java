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

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.runtime.TaskContext;
import org.apache.gobblin.runtime.util.TaskMetrics;


/**
 * A base implementation of {@link TaskIFace}.
 */
public abstract class BaseAbstractTask implements TaskIFace {

  protected WorkUnitState.WorkingState workingState = WorkUnitState.WorkingState.PENDING;
  protected MetricContext metricContext;

  public BaseAbstractTask(TaskContext taskContext) {
    this.metricContext = TaskMetrics.get(taskContext.getTaskState()).getMetricContext();
  }

  /**
   * Overriding methods should call super at the end.
   */
  @Override
  public void run() {
    this.workingState = WorkUnitState.WorkingState.SUCCESSFUL;
  }

  /**
   * Overriding methods should call super at the end.
   */
  @Override
  public void commit() {
    this.workingState = WorkUnitState.WorkingState.SUCCESSFUL;
  }

  @Override
  public State getPersistentState() {
    return new State();
  }

  @Override
  public State getExecutionMetadata() {
    return new State();
  }

  @Override
  public WorkUnitState.WorkingState getWorkingState() {
    return this.workingState;
  }

  @Override
  public void shutdown() {
    if (getWorkingState() == WorkUnitState.WorkingState.PENDING || getWorkingState() == WorkUnitState.WorkingState.RUNNING) {
      this.workingState = WorkUnitState.WorkingState.CANCELLED;
    }
  }

  @Override
  public boolean awaitShutdown(long timeoutMillis) {
    return true;
  }

  @Override
  public String getProgress() {
    return getWorkingState().toString();
  }

  @Override
  public boolean isSpeculativeExecutionSafe() {
    return false;
  }
}
