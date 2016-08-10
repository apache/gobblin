/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */
package gobblin.runtime.api;

import com.google.common.util.concurrent.Service;

/**
 * Defines an implementation which knows how to run a GobblinJob and keep track of the progress.
 */
public interface JobExecutionDriver extends Service, JobExecutionStateListenerContainer {
  /** The job execution ID */
  JobExecution getJobExecution();
  /** The job execution status */
  JobExecutionStatus getJobExecutionStatus();
  /** The job execution state */
  JobExecutionState getJobExecutionState();
}
