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

package org.apache.gobblin.temporal.workflows.timing;

import java.time.Duration;
import java.time.Instant;

import io.temporal.workflow.Workflow;

import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.metrics.event.GobblinEventBuilder;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.temporal.workflows.trackingevent.activity.GobblinTrackingEventActivity;


/**
 * Boiler plate for tracking elapsed time of events that is compatible with {@link Workflow}
 * by using activities to record time
 */
public class TemporalEventTimer implements EventTimer {
  private final GobblinTrackingEventActivity trackingEventActivity;
  private final EventSubmitter eventSubmitter;
  private final GobblinEventBuilder eventBuilder;

  private Instant startTime;

  private TemporalEventTimer(GobblinTrackingEventActivity trackingEventActivity, EventSubmitter eventSubmitter, GobblinEventBuilder eventBuilder) {
    this.trackingEventActivity = trackingEventActivity;
    this.eventBuilder = eventBuilder;
    this.eventSubmitter = eventSubmitter;
    this.startTime = getCurrentTime();
  }

  @Override
  public void close() {
    close(getCurrentTime());
  }

  private void close(Instant endTime) {
    this.eventBuilder.addMetadata(EventSubmitter.EVENT_TYPE, TimingEvent.METADATA_TIMING_EVENT);
    this.eventBuilder.addMetadata(TimingEvent.METADATA_START_TIME, Long.toString(this.startTime.toEpochMilli()));
    this.eventBuilder.addMetadata(TimingEvent.METADATA_END_TIME, Long.toString(endTime.toEpochMilli()));
    Duration duration = Duration.between(this.startTime, endTime);
    this.eventBuilder.addMetadata(TimingEvent.METADATA_DURATION, Long.toString(duration.toMillis()));

    trackingEventActivity.submitGTE(this.eventSubmitter, this.eventBuilder);
  }

  private Instant getCurrentTime() {
    return Instant.ofEpochMilli(Workflow.currentTimeMillis());
  }

  public static class Factory {
    private final GobblinTrackingEventActivity trackingEventActivity;
    private final EventSubmitter eventSubmitter;

    public Factory(GobblinTrackingEventActivity trackingEventActivity, EventSubmitter eventSubmitter) {
      this.trackingEventActivity = trackingEventActivity;
      this.eventSubmitter = eventSubmitter;
    }

    public TemporalEventTimer get(String eventName) {
      GobblinEventBuilder eventBuilder = new GobblinEventBuilder(eventName, eventSubmitter.getNamespace());
      return new TemporalEventTimer(this.trackingEventActivity, eventSubmitter, eventBuilder);
    }

    /**
     * Creates a timer that emits separate events at the start and end of a job
     * @return a timer that emits an event at the beginning of the job and a completion event ends at the end of the job
     */
    public EventTimer getJobTimer() {
      TemporalEventTimer startTimer = get(TimingEvent.LauncherTimings.JOB_START);
      Instant jobStartTime = startTimer.startTime;
      startTimer.close(Instant.EPOCH); // Job start event contains a stub end time

      return () -> {
        TemporalEventTimer endTimer = get(TimingEvent.LauncherTimings.JOB_COMPLETE);
        endTimer.startTime = jobStartTime;
        endTimer.close();
      };
    }
  }
}
