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
import java.util.function.Supplier;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import io.temporal.activity.ActivityOptions;
import io.temporal.workflow.Workflow;

import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.metrics.event.GobblinEventBuilder;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.runtime.util.GsonUtils;


/**
 * Encapsulates emission of {@link org.apache.gobblin.metrics.GobblinTrackingEvent}s, e.g. to convey timing duration and/or event metadata to
 * gobblin-service (GaaS).  For use either within a {@link Workflow} (with emission in a sub-activity, for reliability) or within an
 * {@link io.temporal.activity.Activity} (with direct event emission, since an activity may not itself launch further activities).  That choice
 * is governed by the {@link Factory} used to create the timer: either {@link WithinWorkflowFactory} or {@link WithinActivityFactory}.
 *
 * Implementation Note: While very similar to {@link TimingEvent}, it cannot inherit directly, since its {@link EventSubmitter} field is not serializable.
 * @see EventTimer for details
 */
@RequiredArgsConstructor(access = AccessLevel.PROTECTED)
public class TemporalEventTimer implements EventTimer {
  private final SubmitGTEActivity trackingEventActivity;
  private final GobblinEventBuilder eventBuilder;
  private final EventSubmitterContext eventSubmitterContext;
  private final Supplier<Instant> currentInstantSupplier;
  @Getter private final Instant startTime;

  @Override
  public void stop() {
    stop(currentInstantSupplier.get());
  }

  @Override
  public TemporalEventTimer withMetadata(String key, String metadata) {
    this.eventBuilder.addMetadata(key, metadata);
    return this;
  }

  @Override
  public <T> TemporalEventTimer withMetadataAsJson(String key, T metadata) {
    this.eventBuilder.addMetadata(key, GsonUtils.GSON_WITH_DATE_HANDLING.toJson(metadata));
    return this;
  }

  private void stop(Instant endTime) {
    this.eventBuilder.addMetadata(EventSubmitter.EVENT_TYPE, TimingEvent.METADATA_TIMING_EVENT);
    this.eventBuilder.addMetadata(TimingEvent.METADATA_START_TIME, Long.toString(this.startTime.toEpochMilli()));
    this.eventBuilder.addMetadata(TimingEvent.METADATA_END_TIME, Long.toString(endTime.toEpochMilli()));
    Duration duration = Duration.between(this.startTime, endTime);
    this.eventBuilder.addMetadata(TimingEvent.METADATA_DURATION, Long.toString(duration.toMillis()));

    trackingEventActivity.submitGTE(this.eventBuilder, eventSubmitterContext);
  }


  /**
   * Factory for creating {@link TemporalEventTimer}s, with convenience methods for well-known forms of GaaS-associated
   * {@link org.apache.gobblin.metrics.GobblinTrackingEvent}s.
   *
   * This class is abstract; choose the concrete form befitting the execution context, either {@link WithinWorkflowFactory} or {@link WithinActivityFactory}.
   */
  @RequiredArgsConstructor(access = AccessLevel.PROTECTED)
  public abstract static class Factory {
    private final EventSubmitterContext eventSubmitterContext;
    private final SubmitGTEActivity submitGTEActivity;
    private final Supplier<Instant> currentInstantSupplier;

    public TemporalEventTimer create(String eventName, Instant startTime) {
      GobblinEventBuilder eventBuilder = new GobblinEventBuilder(eventName, eventSubmitterContext.getNamespace());
      return new TemporalEventTimer(this.submitGTEActivity, eventBuilder, this.eventSubmitterContext, currentInstantSupplier, startTime);
    }

    public TemporalEventTimer create(String eventName) {
      return create(eventName, currentInstantSupplier.get());
    }

    /**
     * Utility for creating a timer that emits separate events at the start and end of a job. This imitates the behavior in
     * {@link org.apache.gobblin.runtime.AbstractJobLauncher} by emitting events that are compatible with the
     * {@link org.apache.gobblin.service.monitoring.KafkaAvroJobStatusMonitor}, to update GaaS job/flow status and timing information.
     *
     * @return a timer that will emit a job completion (success) event once `.stop`ped;  *PLUS* immediately emit a job started event
     */
    public TemporalEventTimer createJobTimer() {
      TemporalEventTimer startTimer = create(TimingEvent.LauncherTimings.JOB_START); // update GaaS: `ExecutionStatus.RUNNING`
      startTimer.stop(Instant.EPOCH); // Emit start job event containing a stub end time
      // [upon `.stop()`] update GaaS: `ExecutionStatus.RUNNING`, `TimingEvent.JOB_END_TIME`:
      return create(TimingEvent.LauncherTimings.JOB_SUCCEEDED, startTimer.startTime);
    }

    /**
     * Utility for creating a timer that emits an event to time the start and end of "Work Discovery" (i.e.
     * {@link org.apache.gobblin.source.Source#getWorkunits}).  It SHOULD span only planning - NOT {@link org.apache.gobblin.source.workunit.WorkUnit}
     * preparation, such as serialization, etc.  This imitates the behavior in {@link org.apache.gobblin.runtime.AbstractJobLauncher} by emitting events
     * that are compatible with the {@link org.apache.gobblin.service.monitoring.KafkaAvroJobStatusMonitor}, to update GaaS job timing information, and
     * ultimately participate in the {@link org.apache.gobblin.metrics.GaaSJobObservabilityEvent}.
     *
     * @return a timer that will emit a "work units creation" event once `.stop`ped` (upon (successful) "Work Discovery")
     */
    public TemporalEventTimer createWorkDiscoveryTimer() {
      return create(TimingEvent.LauncherTimings.WORK_UNITS_CREATION); // [upon `.stop()`] update GaaS: `TimingEvent.WORKUNIT_PLAN_{START,END}_TIME`
    }

    /**
     * Utility for creating an event to convey "Work Discovery" metadata, like {@link org.apache.gobblin.source.workunit.WorkUnit} count, size, and
     * bin packing.  To include such info, the caller MUST invoke {@link #withMetadataAsJson(String, Object)} with
     * {@link org.apache.gobblin.temporal.ddm.work.WorkUnitsSizeSummary.Distillation}.
     *
     * The event emitted would inform GaaS of the volume of work discovered (and bin packed), in case `WorkUnit` serialization were to fail for exceeding
     * available memory.  The thence-known amount of work would guide config adjustment for a subsequent re-attempt.
     *
     * IMPORTANT: This event SHOULD be emitted separately from {@link #createWorkDiscoveryTimer()}, to preserve timing accuracy (of that other).
     *
     * @return an event to convey "work units preparation" (Work Discovery) metadata once `.stop`ped
     */
    public TemporalEventTimer createWorkPreparationTimer() {
      // [upon `.stop()`] simply (again) set GaaS: `ExecutionStatus.RUNNING` and convey `TimingEvent.WORKUNITS_GENERATED_SUMMARY` (metadata)
      // (the true purpose in "sending" is to record observable metadata about WU count, size, and bin packing)
      return create(TimingEvent.LauncherTimings.WORK_UNITS_PREPARATION);
    }
  }


  /**
   *  Concrete {@link Factory} to use when executing within a {@link Workflow}.  It will (synchronously) emit the {@link TemporalEventTimer} in an
   *  {@link io.temporal.activity.Activity}, both for reliability and to avoid blocking within the limited `Workflow` time allowance.  In
   *  addition, it uses the `Workflow`-safe {@link Workflow#currentTimeMillis()}.
   */
  public static class WithinWorkflowFactory extends Factory {
    private static final ActivityOptions DEFAULT_OPTS = ActivityOptions.newBuilder()
        .setStartToCloseTimeout(Duration.ofHours(6)) // maximum timeout for the actual event submission to kafka, waiting out a kafka outage
        .build();

    public WithinWorkflowFactory(EventSubmitterContext eventSubmitterContext) {
      this(eventSubmitterContext, DEFAULT_OPTS);
    }

    public WithinWorkflowFactory(EventSubmitterContext eventSubmitterContext, ActivityOptions opts) {
      super(eventSubmitterContext, Workflow.newActivityStub(SubmitGTEActivity.class, opts), WithinWorkflowFactory::getCurrentInstant);
    }

    /**
     * {@link Workflow}-safe (i.e. deterministic) way for equivalent of {@link System#currentTimeMillis()}'s {@link Instant}
     *
     * WARNING: DO NOT use from an {@link io.temporal.activity.Activity}, as that would throw:
     *   Caused by: java.lang.Error: Called from non workflow or workflow callback thread
     *     at io.temporal.internal.sync.DeterministicRunnerImpl.currentThreadInternal(DeterministicRunnerImpl.java:130)
     *     at io.temporal.internal.sync.WorkflowInternal.getRootWorkflowContext(WorkflowInternal.java:404)
     *     at io.temporal.internal.sync.WorkflowInternal.getWorkflowOutboundInterceptor(WorkflowInternal.java:400)
     *     at io.temporal.internal.sync.WorkflowInternal.currentTimeMillis(WorkflowInternal.java:205)
     *     at io.temporal.workflow.Workflow.currentTimeMillis(Workflow.java:524)
     *     ...
     */
    public static Instant getCurrentInstant() {
      return Instant.ofEpochMilli(Workflow.currentTimeMillis());
    }
  }

  /**
   *  Concrete {@link Factory} to use when executing within an {@link io.temporal.activity.Activity}.  It will (synchronously) emit the
   *  {@link TemporalEventTimer} directly within the current `Activity`, since an activity may not itself launch further activities.  It uses
   *  the standard {@link System#currentTimeMillis()}, since `Workflow` determinism is not a concern within an `Activity`.
   */
  public static class WithinActivityFactory extends Factory {
    public WithinActivityFactory(EventSubmitterContext eventSubmitterContext) {
      // reference the `SubmitGTEActivity` impl directly, w/o temporal involved (i.e. NOT via `newActivityStub`), to permit use inside an activity; otherwise:
      //   io.temporal.internal.activity.ActivityTaskExecutors$BaseActivityTaskExecutor  - Activity failure. ActivityId=..., activityType=..., attempt=4
      //   java.lang.Error: Called from non workflow or workflow callback thread
      //     at io.temporal.internal.sync.DeterministicRunnerImpl.currentThreadInternal(DeterministicRunnerImpl.java:130)
      //     at io.temporal.internal.sync.WorkflowInternal.getRootWorkflowContext(WorkflowInternal.java:404)
      //     at io.temporal.internal.sync.WorkflowInternal.newActivityStub(WorkflowInternal.java:239)
      //     at io.temporal.workflow.Workflow.newActivityStub(Workflow.java:92)
      //     at org.apache.gobblin.temporal.workflows.metrics.TemporalEventTimer$Factory.<init>(TemporalEventTimer.java:89)
      //     ...
      super(eventSubmitterContext, new SubmitGTEActivityImpl(), WithinActivityFactory::getCurrentInstant);
    }

    /** @return (standard) {@link System#currentTimeMillis()}'s {@link Instant}, abstracted merely for code clarity */
    public static Instant getCurrentInstant() {
      return Instant.ofEpochMilli(System.currentTimeMillis());
    }
  }
}
