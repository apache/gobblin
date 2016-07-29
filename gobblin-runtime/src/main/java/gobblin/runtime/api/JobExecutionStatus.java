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

import gobblin.runtime.JobState;

import lombok.Getter;

public class JobExecutionStatus {
  public static final String UKNOWN_STAGE = "unkown";

  @Getter final JobExecution jobExecution;
  @Getter JobState.RunningState status;
  /** Arbitrary execution stage, e.g. setup, workUnitGeneration, taskExecution, publishing */
  @Getter String stage;

  // TODO commented out to avoid FindBugs warning
  // transient List<JobExecutionStateListener> changeListeners;

  public JobExecutionStatus(JobExecution jobExecution) {
    this.jobExecution = jobExecution;
    this.status = JobState.RunningState.PENDING;
    this.stage = UKNOWN_STAGE;
  }

}
