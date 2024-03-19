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

import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.runtime.api.DagActionStore;
import org.apache.gobblin.runtime.api.MultiActiveLeaseArbiter;
import org.apache.gobblin.service.modules.orchestration.task.DagTask;
import org.apache.gobblin.service.modules.orchestration.task.LaunchDagTask;
import org.apache.gobblin.util.ConfigUtils;


/**
 * DagManagementTaskStreamImpl implements {@link DagManagement} and {@link DagTaskStream}. It accepts
 * {@link org.apache.gobblin.runtime.api.DagActionStore.DagAction}s and iteratively provides {@link DagTask}.
 */
@Slf4j
@Singleton
@Data
public class DagManagementTaskStreamImpl implements DagManagement, DagTaskStream {
  private final Config config;
  @Getter private final EventSubmitter eventSubmitter;

  @Inject(optional=true)
  protected Optional<DagActionStore> dagActionStore;
  protected Optional<ReminderSettingDagProcLeaseArbiter> reminderSettingDagProcLeaseArbiter;
  @Inject
  private static final int MAX_HOUSEKEEPING_THREAD_DELAY = 180;
  private final BlockingQueue<DagActionStore.DagAction> dagActionQueue = new LinkedBlockingQueue<>();

  // TODO: need to pass reference to DagProcLeaseArbiter without creating a circular reference in Guice
  @Inject
  public DagManagementTaskStreamImpl(Config config, Optional<DagActionStore> dagActionStore,
      Optional<ReminderSettingDagProcLeaseArbiter> reminderSettingDagProcLeaseArbiter) {
    this.config = config;
    this.dagActionStore = dagActionStore;
    this.reminderSettingDagProcLeaseArbiter = reminderSettingDagProcLeaseArbiter;
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
    /*TODO: this requires use of lease arbiter, in single-active execution lease arbiter will not be present and we can
     provide a dummy LeaseObtainedStatus or create alternate route
    */
    if (!this.reminderSettingDagProcLeaseArbiter.isPresent()) {
      throw new RuntimeException("DagManagement not initialized in multi-active execution mode when required.");
    }
    try {
      MultiActiveLeaseArbiter.LeaseAttemptStatus leaseAttemptStatus = null;
      DagActionStore.DagAction dagAction = null;
      while (!(leaseAttemptStatus instanceof MultiActiveLeaseArbiter.LeaseObtainedStatus)) {
        dagAction = this.dagActionQueue.take();  //`take` blocks till element is not available
        // TODO: need to handle reminder events and flag them
        leaseAttemptStatus = this.reminderSettingDagProcLeaseArbiter.get().tryAcquireLease(dagAction, System.currentTimeMillis(), false, false);
      }
      return createDagTask(dagAction, (MultiActiveLeaseArbiter.LeaseObtainedStatus) leaseAttemptStatus);
    } catch (Throwable t) {
      //TODO: need to handle exceptions gracefully
      log.error("Error getting DagAction from the queue / creating DagTask", t);
    }
    return null;
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
}
