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

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import lombok.Data;


/**
 * Submits an event with timings for various stages of a process. The usage is as follows:
 *
 * MultiTimingEvent multiTimer = new MultiTimingEvent(eventSubmitter, "myMultiTimer", false);
 * multiTimer.nextStage("setup");
 * ... // setup
 * multiTimer.nextStage("run");
 * ... // run
 * multiTimer.submit();
 *
 * A call to {@link #nextStage(String)} computes the time spent in the previous stage and
 * starts a new timer for the new stage. A stage can be ended and timed without starting a new stage by calling
 * {@link #endStage()}.
 * When calling {@link #submit()}, the current stage will be ended, and a single event will be send containing the timing
 * for each stage. Calling {@link #close()} will also submit the event.
 * Calling {@link #submit()} or {@link #close()} when the event has already been called will throw an exception.
 *
 * Note: {@link MultiTimingEvent} is not thread-safe, and it should be only used from a single thread.
 *
 */
public class MultiTimingEvent implements Closeable {

  public static final String MULTI_TIMING_EVENT = "multiTimingEvent";

  private final String name;
  private final EventSubmitter submitter;
  private final List<Stage> timings;
  private boolean submitted;
  private final boolean reportAsMetrics;
  private String currentStage;
  private long currentStageStart;

  @Data
  private static class Stage {
    private final String name;
    private final long duration;
  }

  /**
   * @param submitter {@link EventSubmitter} used to send the event.
   * @param name Name of the event to be submitted.
   * @param reportAsMetrics if true, will also report the timing of each stage to a timer with name <namespace>.<name>.<stage>
   */
  public MultiTimingEvent(EventSubmitter submitter, String name, boolean reportAsMetrics) {
    this.name = name;
    this.submitter = submitter;
    this.timings = Lists.newArrayList();
    this.submitted = false;
    this.reportAsMetrics = reportAsMetrics;
  }

  /**
   * End the previous stage, record the time spent in that stage, and start the timer for a new stage.
   * @param name name of the new stage.
   * @throws IOException
   */
  public void nextStage(String name) throws IOException {
    endStage();
    this.currentStage = name;
    this.currentStageStart = System.currentTimeMillis();
  }

  /**
   * End the previous stage and record the time spent in that stage.
   */
  public void endStage() {
    if (this.currentStage != null) {
      long time = System.currentTimeMillis() - this.currentStageStart;
      this.timings.add(new Stage(this.currentStage, time));
      if (reportAsMetrics && submitter.getMetricContext().isPresent()) {
        String timerName = submitter.getNamespace() + "." + name + "." + this.currentStage;
        submitter.getMetricContext().get().timer(timerName).update(time, TimeUnit.MILLISECONDS);
      }
    }
    this.currentStage = null;
  }

  /**
   * See {@link #submit(Map)}. Sends no additional metadata.
   * @throws IOException
   */
  public void submit() throws IOException {
    submit(Maps.<String, String>newHashMap());
  }

  /**
   * Ends the current stage and submits the event containing the timings of each event.
   * @param additionalMetadata additional metadata to include in the event.
   * @throws IOException
   */
  public void submit(Map<String, String> additionalMetadata) throws IOException {

    if (this.submitted) {
      throw new IOException("MultiTimingEvent has already been submitted.");
    }
    this.submitted = true;

    endStage();

    Map<String, String> finalMetadata = Maps.newHashMap();
    finalMetadata.putAll(additionalMetadata);
    finalMetadata.put(EventSubmitter.EVENT_TYPE, MULTI_TIMING_EVENT);
    for (Stage timing : this.timings) {
      finalMetadata.put(timing.getName(), Long.toString(timing.getDuration()));
    }

    this.submitter.submit(this.name, finalMetadata);
  }

  /**
   * Same as {@link #submit()}
   * @throws IOException
   */
  @Override
  public void close()
      throws IOException {
    submit();
  }
}
