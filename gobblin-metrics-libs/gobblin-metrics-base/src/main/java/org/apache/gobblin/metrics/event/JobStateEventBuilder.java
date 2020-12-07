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

import java.util.Map;

import org.apache.commons.lang.StringUtils;

import org.apache.gobblin.metrics.GobblinTrackingEvent;


/**
 * An event reporting job state. It can be used to report the overall Gobblin job state or
 * the state of an internal job, e.g MR job
 */
public class JobStateEventBuilder extends GobblinEventBuilder {

  private static final String JOB_STATE_EVENT_TYPE = "JobStateEvent";
  private static final String STATUS_KEY = "jobState";
  private static final String JOB_URL_KEY = "jobTrackingURL";

  public enum Status {
    SUCCEEDED,
    FAILED
  }

  public Status status;
  public String jobTrackingURL;

  public JobStateEventBuilder(String name) {
    this(name, null);
  }

  public JobStateEventBuilder(String name, String namespace) {
    super(name, namespace);
    this.metadata.put(EVENT_TYPE, JOB_STATE_EVENT_TYPE);
  }

  @Override
  public GobblinTrackingEvent build() {
    if (status != null) {
      this.metadata.put(STATUS_KEY, status.name());
    }
    this.metadata.put(JOB_URL_KEY, jobTrackingURL);
    return super.build();
  }

  public static boolean isJobStateEvent(GobblinTrackingEvent event) {
    String eventType = (event.getMetadata() == null) ? "" : event.getMetadata().get(EVENT_TYPE);
    return StringUtils.isNotEmpty(eventType) && eventType.equals(JOB_STATE_EVENT_TYPE);
  }

  public static JobStateEventBuilder fromEvent(GobblinTrackingEvent event) {
    if(!isJobStateEvent(event)) {
      return null;
    }

    Map<String, String> metadata = event.getMetadata();
    JobStateEventBuilder eventBuilder = new JobStateEventBuilder(event.getName(), event.getNamespace());
    metadata.forEach((key, value) -> {
      switch (key) {
        case STATUS_KEY:
          eventBuilder.status = Status.valueOf(value);
          break;
        case JOB_URL_KEY:
          eventBuilder.jobTrackingURL = value;
          break;
        default:
          eventBuilder.addMetadata(key, value);
          break;
      }
    });

    return eventBuilder;
  }

  public static final class MRJobState {
    public static final String MR_JOB_STATE = "MRJobState";
  }
}
