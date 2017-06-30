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

import java.util.HashMap;
import java.util.Map;

/**
 * Enum for event names
 */
public enum EventName {
  FULL_JOB_EXECUTION("FullJobExecutionTimer"),
  WORK_UNITS_CREATION("WorkUnitsCreationTimer"),
  WORK_UNITS_PREPARATION("WorkUnitsPreparationTimer"),
  JOB_PREPARE("JobPrepareTimer"),
  JOB_START("JobStartTimer"),
  JOB_RUN("JobRunTimer"),
  JOB_COMMIT("JobCommitTimer"),
  JOB_CLEANUP("JobCleanupTimer"),
  JOB_CANCEL("JobCancelTimer"),
  JOB_COMPLETE("JobCompleteTimer"),
  JOB_FAILED("JobFailedTimer"),
  MR_STAGING_DATA_CLEAN("JobMrStagingDataCleanTimer"),
  UNKNOWN("Unknown");

  // for mapping event id to enum object
  private static final Map<String,EventName> idMap;

  static {
    idMap = new HashMap<String, EventName>();
    for (EventName value : EventName.values()) {
      idMap.put(value.eventId, value);
    }
  }

  private String eventId;

  EventName(String eventId) {
    this.eventId = eventId;
  }

  public String getEventId() {
    return eventId;
  }

  public static EventName getEnumFromEventId(String eventId) {
    EventName eventNameEnum = idMap.get(eventId);

    return eventNameEnum == null ? UNKNOWN : eventNameEnum;
  }
}
