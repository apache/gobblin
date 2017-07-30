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
package gobblin.runtime.api;

import java.net.URI;
import java.util.Map;

import gobblin.annotation.Alpha;

/**
 *  Early alpha of the scheduler interface which is responsible of deciding when to run next the
 *  GobblinJob identified by a JobSpec. The scheduler maintains one {@link JobSpecSchedule} per
 *  JobSpec URI. Which means that scheduling a new version of JobSpec will override the schedule of
 *  the previous version.
 *
 *  The scheduler is fire-and-forget. It is caller's responsibility (typically,
 *  {@link GobblinInstanceDriver}) to keep track of the outcome of the execution.
 */
@Alpha
public interface JobSpecScheduler extends JobSpecSchedulerListenersContainer {
  /**
   * Add a Gobblin job for scheduling. If the job is configured appropriately (scheduler-dependent),
   * it will be executed repeatedly.
   *
   * @param   jobSpec     the JobSpec of the job
   * @param   jobRunnable a runnable that will execute the job
   */
  public JobSpecSchedule scheduleJob(JobSpec jobSpec, Runnable jobRunnable);

  /**
   * Add a Gobblin job for scheduling. Job is guaranteed to run only once regardless of job outcome.
   *
   * @param   jobSpec     the JobSpec of the job
   * @param   jobRunnable a runnable that will execute the job
   */
  public JobSpecSchedule scheduleOnce(JobSpec jobSpec, Runnable jobRunnable);

  /**
   * Remove a job from scheduling. This will not affect any executions that are currently running.
   * @param jobSpecURI    the URI of the Gobblin job to unschedule.
   * */
  public void unscheduleJob(URI jobSpecURI);

  /** The outstanding job schedules. This is represents a snapshot in time and will not be updated
   * as new schedules are added/removed.
   * @return a map with the URIs of the scheduled jobs for keys and the schedules for values */
  public Map<URI, JobSpecSchedule> getSchedules();
}
