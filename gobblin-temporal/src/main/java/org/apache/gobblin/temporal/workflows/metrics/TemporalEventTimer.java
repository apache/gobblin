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

package org.apache.gobblin.temporal.workflows.metrics;

import java.time.Duration;
import java.time.Instant;

import io.temporal.workflow.Workflow;
import lombok.RequiredArgsConstructor;

import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.metrics.event.GobblinEventBuilder;
import org.apache.gobblin.metrics.event.TimingEvent;


/**
 * Boiler plate for tracking elapsed time of events that is compatible with {@link Workflow}
 * by using activities to record time
 *
 * This class is very similar to {@link TimingEvent} but uses {@link Workflow} compatible APIs. It's possible to refactor
 * this class to inherit the {@link TimingEvent} but extra care would be needed to remove the {@link EventSubmitter} field
 * since that class is not serializable without losing some information
 */
@RequiredArgsConstructor
public class TemporalEventTimer implements EventTimer {
  private final SubmitGTEActivity trackingEventActivity;
  private final GobblinEventBuilder eventBuilder;
  private final TrackingEventMetadata trackingEventMetadata;
  private final Instant startTime;

  @Override
  public void stop() {
    stop(getCurrentTime());
  }

  private void stop(Instant endTime) {
    this.eventBuilder.addMetadata(EventSubmitter.EVENT_TYPE, TimingEvent.METADATA_TIMING_EVENT);
    this.eventBuilder.addMetadata(TimingEvent.METADATA_START_TIME, Long.toString(this.startTime.toEpochMilli()));
    this.eventBuilder.addMetadata(TimingEvent.METADATA_END_TIME, Long.toString(endTime.toEpochMilli()));
    Duration duration = Duration.between(this.startTime, endTime);
    this.eventBuilder.addMetadata(TimingEvent.METADATA_DURATION, Long.toString(duration.toMillis()));

    trackingEventActivity.submitGTE(this.eventBuilder, trackingEventMetadata);
  }

  private static Instant getCurrentTime() {
    return Instant.ofEpochMilli(Workflow.currentTimeMillis());
  }

  public static class Factory {
    private final SubmitGTEActivity submitGTEActivity;
    private final TrackingEventMetadata trackingEventMetadata;

    public Factory(SubmitGTEActivity submitGTEActivity, TrackingEventMetadata trackingEventMetadata) {
        this.submitGTEActivity = submitGTEActivity;
        this.trackingEventMetadata = trackingEventMetadata;
    }

    public TemporalEventTimer create(String eventName, Instant startTime) {
      GobblinEventBuilder eventBuilder = new GobblinEventBuilder(eventName, trackingEventMetadata.getNamespace());
      return new TemporalEventTimer(submitGTEActivity, eventBuilder, this.trackingEventMetadata, startTime);
    }

    public TemporalEventTimer create(String eventName) {
      return create(eventName, getCurrentTime());
    }

    /**
     * Creates a timer that emits separate events at the start and end of a job. This imitates the behavior in
     * {@link org.apache.gobblin.runtime.AbstractJobLauncher} by using stubs to be compatible with the
     * {@link org.apache.gobblin.runtime.job_monitor.KafkaAvroJobMonitor}
     *
     * @return a timer that emits an event at the beginning of the job and a completion event ends at the end of the job
     */
    public EventTimer getJobTimer() {
      TemporalEventTimer startTimer = create(TimingEvent.LauncherTimings.JOB_START);
      startTimer.stop(Instant.EPOCH); // Job start event contains a stub end time

      return () -> {
        TemporalEventTimer endTimer = create(TimingEvent.LauncherTimings.JOB_COMPLETE, startTimer.startTime);
        endTimer.stop();
      };
    }
  }
}
