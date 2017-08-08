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

package org.apache.gobblin.metrics.event;

import org.apache.gobblin.metrics.GobblinTrackingEvent;

/**
 * Task-related event types and their metadata, stored in {@link GobblinTrackingEvent#metadata}
 *
 * @author Lorand Bendig
 *
 */
public class TaskEvent {

  public static final String TASK_STATE = "TaskStateEvent";
  public static final String TASK_FAILED = "TaskFailed";
  public static final String TASK_COMMITTED_EVENT_NAME = "taskCommitted";

  public static final String METADATA_TASK_ID = "taskId";
  public static final String METADATA_TASK_ATTEMPT_ID = "taskAttemptId";
  public static final String METADATA_TASK_START_TIME = "taskStartTime";
  public static final String METADATA_TASK_END_TIME = "taskEndTime";
  public static final String METADATA_TASK_WORKING_STATE = "taskWorkingState";
  public static final String METADATA_TASK_FAILURE_CONTEXT = "taskFailureContext";

}
