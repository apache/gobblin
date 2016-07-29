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
package gobblin.runtime.api;

import java.util.HashMap;
import java.util.Map;

import lombok.Getter;

/**
 * TODO
 *
 */
public class JobExecutionState extends JobExecutionStatus {
  @Getter final JobSpec jobSpec;
  Map<String, Object> executionMetadata;

  public JobExecutionState(JobSpec jobSpec, JobExecution jobExecution) {
    super(jobExecution);
    this.jobSpec = jobSpec;
    // TODO default implementation
    this.executionMetadata = new HashMap<>();
  }

  public Map<String, Object> getExecutionMetadata() {
    return this.executionMetadata;
  }

}
