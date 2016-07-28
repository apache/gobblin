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
package gobblin.runtime.std;

import java.net.URI;

import gobblin.annotation.Alpha;
import gobblin.runtime.api.JobExecution;
import gobblin.runtime.api.JobSpec;

import lombok.Data;

/**
 * Identifies a specific execution of a {@link JobSpec}
 */
@Alpha
@Data
public class JobExecutionUpdatable implements JobExecution {
  /** The URI of the job being executed */
  protected URI jobSpecURI;
  /** The version of the JobSpec being launched */
  protected String jobSpecVersion;
  /** The millisecond timestamp when the job was launched */
  protected long launchTimeMillis;
  /** Unique (for the given JobExecutionLauncher) id for this execution */
  protected String executionId;
}
