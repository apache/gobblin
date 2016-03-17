/*
 * Copyright (C) 2016 Swisscom All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
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
  
  public static String JOB_STATE = "JobStateEvent";
  public static String LOCK_IN_USE = "LockInUse";
  public static String WORK_UNITS_MISSING = "WorkUnitsMissing";
  public static String WORK_UNITS_EMPTY = "WorkUnitsEmpty";
  public static String TASKS_SUBMITTED = "TasksSubmitted";

  public static String METADATA_JOB_ID = "jobId";
  public static String METADATA_JOB_NAME = "jobName";
  public static String METADATA_JOB_START_TIME = "jobBeginTime";
  public static String METADATA_JOB_END_TIME = "jobEndTime";
  public static String METADATA_JOB_STATE = "jobState";
  public static String METADATA_JOB_LAUNCHED_TASKS = "jobLaunchedTasks";
  public static String METADATA_JOB_COMPLETED_TASKS = "jobCompletedTasks";
  public static String METADATA_JOB_LAUNCHER_TYPE = "jobLauncherType";
  public static String METADATA_JOB_TRACKING_URL = "jobTrackingURL";

}
