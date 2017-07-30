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

package gobblin.metrics.event;

import gobblin.metrics.GobblinTrackingEvent;

/**
 * Job-related event types and their metadata, stored in {@link GobblinTrackingEvent#metadata}
 *
 * @author Lorand Bendig
 *
 */
public class JobEvent {

  public static final String JOB_STATE = "JobStateEvent";
  public static final String LOCK_IN_USE = "LockInUse";
  public static final String WORK_UNITS_MISSING = "WorkUnitsMissing";
  public static final String WORK_UNITS_EMPTY = "WorkUnitsEmpty";
  public static final String TASKS_SUBMITTED = "TasksSubmitted";

  public static final String METADATA_JOB_ID = "jobId";
  public static final String METADATA_JOB_NAME = "jobName";
  public static final String METADATA_JOB_START_TIME = "jobBeginTime";
  public static final String METADATA_JOB_END_TIME = "jobEndTime";
  public static final String METADATA_JOB_STATE = "jobState";
  public static final String METADATA_JOB_LAUNCHED_TASKS = "jobLaunchedTasks";
  public static final String METADATA_JOB_COMPLETED_TASKS = "jobCompletedTasks";
  public static final String METADATA_JOB_LAUNCHER_TYPE = "jobLauncherType";
  public static final String METADATA_JOB_TRACKING_URL = "jobTrackingURL";

}
