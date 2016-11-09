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

import java.net.URI;

import gobblin.annotation.Alpha;

/**
 * Identifies a specific execution of a {@link JobSpec}
 */
@Alpha
public interface JobExecution {
  /** The URI of the job being executed */
  URI getJobSpecURI();
  /** The version of the JobSpec being launched */
  String getJobSpecVersion();
  /** The millisecond timestamp when the job was launched */
  long getLaunchTimeMillis();
  /** Unique (for the given JobExecutionLauncher) id for this execution */
  String getExecutionId();
}
