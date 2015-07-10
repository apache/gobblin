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
 * Event to time actions in the program. Automatically reports start time, end time, and duration from the time
 * the {@link gobblin.metrics.event.TimingEvent} was created to the time {@link #stop} is called.
 */
public class TimingEvent {

  public static final String START_TIME = "startTime";
  public static final String END_TIME = "endTime";
  public static final String DURATION = "durationMillis";
  public static final String TIMING_EVENT = "timingEvent";

  private final String name;
  private final Long startTime;
  private final EventSubmitter submitter;
  private boolean stopped;

  public TimingEvent(EventSubmitter submitter, String name) {
    this.stopped = false;
    this.name = name;
    this.submitter = submitter;
    this.startTime = System.currentTimeMillis();
  }

  /**
   * Stop the timer and submit the event. If timer was already stopped before, this is a no-op.
   */
  public void stop() {
    if(this.stopped) {
      return;
    }
    this.stopped = true;
    long endTime = System.currentTimeMillis();
    long duration = endTime - this.startTime;
    this.submitter.submit(this.name, EventSubmitter.EVENT_TYPE, TIMING_EVENT, START_TIME, Long.toString(this.startTime),
        END_TIME, Long.toString(endTime), DURATION, Long.toString(duration));
  }
}
