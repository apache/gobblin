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
 * Task-related event types and their metadata, stored in {@link GobblinTrackingEvent#metadata}
 *
 * @author Lorand Bendig
 *
 */
public class TaskEvent {

  public static final String TASK_STATE = "TaskStateEvent";
  public static final String TASK_FAILED = "TaskFailed";

  public static final String METADATA_TASK_ID = "taskId";
  public static final String METADATA_TASK_START_TIME = "taskStartTime";
  public static final String METADATA_TASK_END_TIME = "taskEndTime";
  public static final String METADATA_TASK_WORKING_STATE = "taskWorkingState";
  public static final String METADATA_TASK_FAILURE_CONTEXT = "taskFailureContext";

}
