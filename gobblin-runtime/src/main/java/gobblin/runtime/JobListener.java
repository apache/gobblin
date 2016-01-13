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

package gobblin.runtime;

/**
 * An interface for classes used for callback on job state changes.
 */
public interface JobListener {

  /**
   * Called when a job is completed.
   *
   * @param jobState a {@link JobState} object
   */
  public void onJobCompletion(JobState jobState);

  /**
   * Called when a job is cancelled.
   *
   * @param jobState a {@link JobState} object
   */
  public void onJobCancellation(JobState jobState);
}
