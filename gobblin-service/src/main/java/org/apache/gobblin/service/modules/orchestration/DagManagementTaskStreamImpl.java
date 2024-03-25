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

package org.apache.gobblin.service.modules.orchestration;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.quartz.SchedulerException;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import javax.inject.Named;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.runtime.api.DagActionStore;
import org.apache.gobblin.runtime.api.InstrumentedLeaseArbiter;
import org.apache.gobblin.runtime.api.MultiActiveLeaseArbiter;
import org.apache.gobblin.runtime.util.InjectionNames;
import org.apache.gobblin.service.modules.orchestration.task.DagTask;
import org.apache.gobblin.service.modules.orchestration.task.LaunchDagTask;
import org.apache.gobblin.util.ConfigUtils;


/**
 * DagManagementTaskStreamImpl implements {@link DagManagement} and {@link DagTaskStream}. It accepts
 * {@link org.apache.gobblin.runtime.api.DagActionStore.DagAction}s and iteratively provides {@link DagTask}.
 *
 * If multi-active execution is enabled, then it uses {@link MultiActiveLeaseArbiter} to coordinate multiple hosts with
 * execution components enabled to respond to flow action events by attempting ownership over a flow action event at a
 * given event time. Only events that the current instance acquires a lease for are selected by
 * {@link DagManagementTaskStreamImpl#next()}. If the status of the lease ownership attempt is anything other than an
 * indication the lease has been completed
 * ({@link org.apache.gobblin.runtime.api.MultiActiveLeaseArbiter.NoLongerLeasingStatus}) then the
 * {@link MultiActiveLeaseArbiter#tryAcquireLease} method will set a reminder for the flow action using
 * {@link DagActionReminderScheduler} to reattempt the lease after the current leaseholder's grant would have expired.
 *
 * If multi-active execution is NOT enabled, then all flow action events are selected by
 * {@link DagManagementTaskStreamImpl#next()} for processing. A dummy
 * {@link org.apache.gobblin.runtime.api.MultiActiveLeaseArbiter.LeaseObtainedStatus} is provided without utilizing a
 * lease arbiter that is initialized with a lease that will never expire (or as close to it as possible with long max
 * value).
 */
@Slf4j
@Singleton
@Data
public class DagManagementTaskStreamImpl implements DagManagement, DagTaskStream {
  private final Config config;
  @Getter private final EventSubmitter eventSubmitter;

  @Inject(optional=true)
  protected Optional<DagActionStore> dagActionStore;
  @Inject
  @Named(ConfigurationKeys.EXECUTOR_LEASE_ARBITER_NAME)
  protected InstrumentedLeaseArbiter dagActionExecutionLeaseArbiter;
  protected Optional<DagActionReminderScheduler> dagActionReminderScheduler;
  @Inject
  @Named(InjectionNames.MULTI_ACTIVE_EXECUTION_ENABLED)
  private final boolean isMultiActiveExecutionEnabled;
  @Inject
  private static final int MAX_HOUSEKEEPING_THREAD_DELAY = 180;
  private final BlockingQueue<DagActionStore.DagAction> dagActionQueue = new LinkedBlockingQueue<>();
  private final String MISSING_OPTIONAL_ERROR_MESSAGE = String.format("Multi-active execution enabled but required "
      + "instances of %s and %s are absent.", MultiActiveLeaseArbiter.class.getSimpleName(), DagActionReminderScheduler.class.getSimpleName());

  @Inject
  public DagManagementTaskStreamImpl(Config config, Optional<DagActionStore> dagActionStore,
      @Named(ConfigurationKeys.EXECUTOR_LEASE_ARBITER_NAME) InstrumentedLeaseArbiter dagActionExecutionLeaseArbiter,
      Optional<DagActionReminderScheduler> dagActionReminderScheduler,
      @Named(InjectionNames.MULTI_ACTIVE_EXECUTION_ENABLED) boolean isMultiActiveExecutionEnabled) {
    this.config = config;
    this.dagActionStore = dagActionStore;
    this.dagActionExecutionLeaseArbiter = dagActionExecutionLeaseArbiter;
    this.dagActionReminderScheduler = dagActionReminderScheduler;
    this.isMultiActiveExecutionEnabled = isMultiActiveExecutionEnabled;
    MetricContext metricContext = Instrumented.getMetricContext(ConfigUtils.configToState(ConfigFactory.empty()), getClass());
    this.eventSubmitter = new EventSubmitter.Builder(metricContext, "org.apache.gobblin.service").build();
  }

  @Override
  public synchronized void addDagAction(DagActionStore.DagAction dagAction) {
    // TODO: Used to track missing dag issue, remove later as needed
    log.info("Add dagAction{}", dagAction);

    if (!this.dagActionQueue.offer(dagAction)) {
      throw new RuntimeException("Could not add dag action " + dagAction + " to the queue");
    }
  }

  @Override
  public boolean hasNext() {
    return true;
  }

  @Override
  public DagTask next() {
    if (this.isMultiActiveExecutionEnabled && !this.dagActionReminderScheduler.isPresent()) {
      throw new RuntimeException(MISSING_OPTIONAL_ERROR_MESSAGE);
    }
    try {
      MultiActiveLeaseArbiter.LeaseAttemptStatus leaseAttemptStatus = null;
      DagActionStore.DagAction dagAction = null;
      while (!(leaseAttemptStatus instanceof MultiActiveLeaseArbiter.LeaseObtainedStatus)) {
        dagAction = this.dagActionQueue.take();  //`take` blocks till element is not available
        leaseAttemptStatus = retrieveLeaseStatus(dagAction);
      }
      return createDagTask(dagAction, (MultiActiveLeaseArbiter.LeaseObtainedStatus) leaseAttemptStatus);
    } catch (Throwable t) {
      //TODO: need to handle exceptions gracefully
      log.error("Error getting DagAction from the queue / creating DagTask", t);
    }
    return null;
  }

  /**
   * Returns a {@link org.apache.gobblin.runtime.api.MultiActiveLeaseArbiter.LeaseAttemptStatus} associated with the
   * `dagAction`. If in multi-active execution mode, it retrieves the status from calling
   * {@link MultiActiveLeaseArbiter#tryAcquireLease(DagActionStore.DagAction, long, boolean, boolean)}, otherwise
   * it returns a {@link org.apache.gobblin.runtime.api.MultiActiveLeaseArbiter.LeaseObtainedStatus} that will not
   * expire for a very long time to the current instance.
   * @param dagAction
   * @return
   * @throws IOException
   * @throws SchedulerException
   */
  private MultiActiveLeaseArbiter.LeaseAttemptStatus retrieveLeaseStatus(DagActionStore.DagAction dagAction)
      throws IOException, SchedulerException {
    MultiActiveLeaseArbiter.LeaseAttemptStatus leaseAttemptStatus;
    if (!this.isMultiActiveExecutionEnabled) {
      leaseAttemptStatus = new MultiActiveLeaseArbiter.LeaseObtainedStatus(dagAction, System.currentTimeMillis(), Long.MAX_VALUE, null);
    } else {
      // TODO: need to handle reminder events and flag them
      leaseAttemptStatus = this.dagActionExecutionLeaseArbiter
          .tryAcquireLease(dagAction, System.currentTimeMillis(), false, false);
          /* Schedule a reminder for the event unless the lease has been completed to safeguard against the case where even
          we, when we might become the lease owner still fail to complete processing
          */
      if (!(leaseAttemptStatus instanceof MultiActiveLeaseArbiter.NoLongerLeasingStatus)) {
        scheduleReminderForEvent(leaseAttemptStatus);
      }
    }
    return leaseAttemptStatus;
  }

  private DagTask createDagTask(DagActionStore.DagAction dagAction, MultiActiveLeaseArbiter.LeaseObtainedStatus leaseObtainedStatus) {
    DagActionStore.DagActionType dagActionType = dagAction.getDagActionType();

    switch (dagActionType) {
      case LAUNCH:
        return new LaunchDagTask(dagAction, leaseObtainedStatus);
      default:
        throw new UnsupportedOperationException("Not yet implemented");
    }
  }

  /* Schedules a reminder for the flow action using {@link DagActionReminderScheduler} to reattempt the lease after the
  current leaseholder's grant would have expired.
  */
  protected void scheduleReminderForEvent(MultiActiveLeaseArbiter.LeaseAttemptStatus leaseStatus)
      throws SchedulerException {
    if (!this.dagActionReminderScheduler.isPresent()) {
      throw new RuntimeException(MISSING_OPTIONAL_ERROR_MESSAGE);
    }
    dagActionReminderScheduler.get().scheduleReminder(leaseStatus.getDagAction(),
        leaseStatus.getMinimumLingerDurationMillis());
  }
}
