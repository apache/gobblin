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

package gobblin.metrics.event;

/**
 * Constants used as names for {@link gobblin.metrics.GobblinTrackingEvent}s.
 */
public class EventNames {

  public static final String LOCK_IN_USE = "LockInUse";
  public static final String WORK_UNITS_MISSING = "WorkUnitsMissing";
  public static final String WORK_UNITS_EMPTY = "WorkUnitsEmpty";
  public static final String TASKS_SUBMITTED = "TasksSubmitted";
  public static final String TASK_FAILED = "TaskFailed";

}
