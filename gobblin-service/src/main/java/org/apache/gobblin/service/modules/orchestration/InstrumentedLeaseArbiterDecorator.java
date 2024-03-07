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

import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.ContextAwareCounter;
import org.apache.gobblin.metrics.ContextAwareMeter;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.ServiceMetricNames;
import org.apache.gobblin.runtime.api.DagActionStore;
import org.apache.gobblin.runtime.api.MultiActiveLeaseArbiter;
import org.apache.gobblin.runtime.api.MysqlMultiActiveLeaseArbiter;
import org.apache.gobblin.util.ConfigUtils;


/*
  A generic lease arbitration decorator built upon the {@link MysqlMultiActiveLeaseArbiter} which encapsulates common
  functionality desired by lease arbiter users to track metrics on all lease attempts made by the arbiter. The metrics
  can be used to compare relative performance among arbitration participants.
 */
@Slf4j
public abstract class InstrumentedLeaseArbiterDecorator extends MysqlMultiActiveLeaseArbiter {
  protected MultiActiveLeaseArbiter multiActiveLeaseArbiter;
  protected MetricContext metricContext;
  private ContextAwareCounter leaseObtainedCount;

  private ContextAwareCounter leasedToAnotherStatusCount;

  private ContextAwareCounter noLongerLeasingStatusCount;
  private ContextAwareMeter leasesObtainedDueToReminderCount;

  public InstrumentedLeaseArbiterDecorator(Config config, MultiActiveLeaseArbiter leaseDeterminationStore,
      String metricsPrefix) throws IOException {
    super(config);
    this.multiActiveLeaseArbiter = leaseDeterminationStore;
    this.metricContext = Instrumented.getMetricContext(new org.apache.gobblin.configuration.State(ConfigUtils.configToProperties(config)),
        this.getClass());
    initializeMetrics(metricsPrefix);
  }

  private void initializeMetrics(String metricsPrefix) {
    // If a valid metrics prefix is provided then add a delimiter after it
    if (metricsPrefix != "") {
      metricsPrefix += ".";
    }
    this.leaseObtainedCount = this.metricContext.contextAwareCounter(metricsPrefix + ServiceMetricNames.FLOW_TRIGGER_HANDLER_LEASE_OBTAINED_COUNT);
    this.leasedToAnotherStatusCount = this.metricContext.contextAwareCounter(metricsPrefix + ServiceMetricNames.FLOW_TRIGGER_HANDLER_LEASED_TO_ANOTHER_COUNT);
    this.noLongerLeasingStatusCount = this.metricContext.contextAwareCounter(metricsPrefix + ServiceMetricNames.FLOW_TRIGGER_HANDLER_NO_LONGER_LEASING_COUNT);
    this.leasesObtainedDueToReminderCount = this.metricContext.contextAwareMeter(metricsPrefix + ServiceMetricNames.FLOW_TRIGGER_HANDLER_LEASES_OBTAINED_DUE_TO_REMINDER_COUNT);
  }

  /**
   * This method is used by the multi-active scheduler and multi-active execution classes (DagTaskStream) to attempt a
   * lease for a particular job event and return the status of the attempt.
   * @param flowAction
   * @param eventTimeMillis
   * @param isReminderEvent
   * @param skipFlowExecutionIdReplacement
   * @return
   */
  @Override
  public MultiActiveLeaseArbiter.LeaseAttemptStatus tryAcquireLease(DagActionStore.DagAction flowAction, long eventTimeMillis,
      boolean isReminderEvent, boolean skipFlowExecutionIdReplacement) throws IOException {

    MultiActiveLeaseArbiter.LeaseAttemptStatus leaseAttemptStatus =
        multiActiveLeaseArbiter.tryAcquireLease(flowAction, eventTimeMillis, isReminderEvent,
            skipFlowExecutionIdReplacement);
    // The flow action contained in the`LeaseAttemptStatus` from the lease arbiter contains an updated flow execution
    // id. From this point onwards, always use the newer version of the flow action to easily track the action through
    // orchestration and execution.
    if (leaseAttemptStatus instanceof MultiActiveLeaseArbiter.LeaseObtainedStatus) {
      if (isReminderEvent) {
        this.leasesObtainedDueToReminderCount.mark();
      }
      this.leaseObtainedCount.inc();
      return leaseAttemptStatus;
    } else if (leaseAttemptStatus instanceof MultiActiveLeaseArbiter.LeasedToAnotherStatus) {
      this.leasedToAnotherStatusCount.inc();
//        scheduleReminderForEvent((MultiActiveLeaseArbiter.LeasedToAnotherStatus) leaseAttemptStatus, eventTimeMillis);
      return leaseAttemptStatus;
    } else if (leaseAttemptStatus instanceof MultiActiveLeaseArbiter.NoLongerLeasingStatus) {
      this.noLongerLeasingStatusCount.inc();
      log.debug("Received type of leaseAttemptStatus: [{}, eventTimestamp: {}] ", leaseAttemptStatus.getClass().getName(),
          eventTimeMillis);
      return leaseAttemptStatus;
    }
    throw new RuntimeException(String.format("Received type of leaseAttemptStatus: %s not handled by this method",
        leaseAttemptStatus.getClass().getName()));
  }
}
