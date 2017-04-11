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

import static gobblin.metrics.event.JobEvent.JOB_STATE;
import static gobblin.metrics.event.JobEvent.METADATA_JOB_COMPLETED_TASKS;
import static gobblin.metrics.event.JobEvent.METADATA_JOB_END_TIME;
import static gobblin.metrics.event.JobEvent.METADATA_JOB_LAUNCHED_TASKS;
import static gobblin.metrics.event.JobEvent.METADATA_JOB_START_TIME;
import static gobblin.metrics.event.JobEvent.METADATA_JOB_STATE;
import static gobblin.metrics.event.TaskEvent.METADATA_TASK_END_TIME;
import static gobblin.metrics.event.TaskEvent.METADATA_TASK_START_TIME;
import static gobblin.metrics.event.TaskEvent.METADATA_TASK_WORKING_STATE;
import static gobblin.metrics.event.TaskEvent.TASK_STATE;
import static gobblin.metrics.event.TimingEvent.METADATA_DURATION;
import static gobblin.metrics.event.TimingEvent.METADATA_END_TIME;
import static gobblin.metrics.event.TimingEvent.METADATA_START_TIME;
import static gobblin.metrics.event.TimingEvent.METADATA_TIMING_EVENT;

import java.util.Map;

import com.google.common.collect.ImmutableMap;

/**
 * Each of the metadata fields of a MultiPartEvent is reported separately
 *
 * @author Lorand Bendig
 *
 */
public enum MultiPartEvent {

  TIMING_EVENT(
      METADATA_TIMING_EVENT,
      METADATA_START_TIME,
      METADATA_END_TIME,
      METADATA_DURATION),
  JOBSTATE_EVENT(
      JOB_STATE,
      METADATA_JOB_START_TIME,
      METADATA_JOB_END_TIME,
      METADATA_JOB_LAUNCHED_TASKS,
      METADATA_JOB_COMPLETED_TASKS,
      METADATA_JOB_STATE),
  TASKSTATE_EVENT(
      TASK_STATE,
      METADATA_TASK_START_TIME,
      METADATA_TASK_END_TIME,
      METADATA_TASK_WORKING_STATE);

  private String eventName;
  private String[] metadataFields;

  private static final Map<String, MultiPartEvent> lookup;
  static {
    ImmutableMap.Builder<String, MultiPartEvent> builder = new  ImmutableMap.Builder<String, MultiPartEvent>();
    for (MultiPartEvent event : MultiPartEvent.values()) {
      builder.put(event.eventName, event);
    }
    lookup = builder.build();
  }

  private MultiPartEvent(String eventName, String... metadataFields) {
    this.eventName = eventName;
    this.metadataFields = metadataFields;
  }

  public static MultiPartEvent getEvent(String eventName) {
    return lookup.get(eventName);
  }

  public String getEventName() {
    return eventName;
  }

  public String[] getMetadataFields() {
    return metadataFields;
  }

}
