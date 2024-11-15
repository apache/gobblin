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

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.ContextAwareCounter;
import org.apache.gobblin.metrics.ContextAwareMeter;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.ServiceMetricNames;
import org.apache.gobblin.util.ConfigUtils;


/*
  A {@link MultiActiveLeaseArbiter} decorator for tracking metrics about lease attempts. The metrics can be used to
  compare relative performance, such as collisions/contention, among arbitration participants.
 */
@Slf4j
public class InstrumentedLeaseArbiter implements MultiActiveLeaseArbiter {
  private final MultiActiveLeaseArbiter decoratedMultiActiveLeaseArbiter;
  @Getter
  protected MetricContext metricContext;
  private ContextAwareCounter leaseObtainedCount;

  private ContextAwareCounter leasedToAnotherStatusCount;

  private ContextAwareCounter noLongerLeasingStatusCount;
  private ContextAwareMeter leasesObtainedDueToReminderCount;
  private ContextAwareMeter recordedLeaseSuccessCount;
  private ContextAwareMeter failedToRecordLeaseSuccessCount;

  public InstrumentedLeaseArbiter(Config config, MultiActiveLeaseArbiter leaseDeterminationStore,
      String metricsPrefix) {
    this.decoratedMultiActiveLeaseArbiter = leaseDeterminationStore;
    this.metricContext = Instrumented.getMetricContext(new org.apache.gobblin.configuration.State(ConfigUtils.configToProperties(config)),
        this.getClass());
    initializeMetrics(metricsPrefix);
  }

  private void initializeMetrics(String metricsPrefix) {
    // If a valid metrics prefix is provided then add a delimiter after it
    if (!metricsPrefix.isEmpty()) {
      metricsPrefix = metricsPrefix.concat(".");
    }
    this.leaseObtainedCount = this.metricContext.contextAwareCounter(metricsPrefix + ServiceMetricNames.FLOW_TRIGGER_HANDLER_LEASE_OBTAINED_COUNT);
    this.leasedToAnotherStatusCount = this.metricContext.contextAwareCounter(metricsPrefix + ServiceMetricNames.FLOW_TRIGGER_HANDLER_LEASED_TO_ANOTHER_COUNT);
    this.noLongerLeasingStatusCount = this.metricContext.contextAwareCounter(metricsPrefix + ServiceMetricNames.FLOW_TRIGGER_HANDLER_NO_LONGER_LEASING_COUNT);
    this.leasesObtainedDueToReminderCount = this.metricContext.contextAwareMeter(metricsPrefix + ServiceMetricNames.FLOW_TRIGGER_HANDLER_LEASES_OBTAINED_DUE_TO_REMINDER_COUNT);
    this.recordedLeaseSuccessCount = metricContext.contextAwareMeter(ServiceMetricNames.FLOW_TRIGGER_HANDLER_RECORDED_LEASE_SUCCESS_COUNT);
    this.failedToRecordLeaseSuccessCount = metricContext.contextAwareMeter(ServiceMetricNames.FLOW_TRIGGER_HANDLER_FAILED_TO_RECORD_LEASE_SUCCESS_COUNT);
  }

  @Override
  public LeaseAttemptStatus tryAcquireLease(DagActionStore.LeaseParams leaseParams, boolean skipFlowExecutionIdReplacement) throws IOException {
    LeaseAttemptStatus leaseAttemptStatus = decoratedMultiActiveLeaseArbiter.tryAcquireLease(leaseParams, skipFlowExecutionIdReplacement);
    if (leaseAttemptStatus instanceof LeaseAttemptStatus.LeaseObtainedStatus) {
      if (leaseParams.isReminder()) {
        this.leasesObtainedDueToReminderCount.mark();
      }
      this.leaseObtainedCount.inc();
      return leaseAttemptStatus;
    } else if (leaseAttemptStatus instanceof LeaseAttemptStatus.LeasedToAnotherStatus) {
      this.leasedToAnotherStatusCount.inc();
      return leaseAttemptStatus;
    } else if (leaseAttemptStatus instanceof LeaseAttemptStatus.NoLongerLeasingStatus) {
      this.noLongerLeasingStatusCount.inc();
      return leaseAttemptStatus;
    }
    throw new RuntimeException(String.format("Unexpected LeaseAttemptStatus (%s) for %s", leaseAttemptStatus.getClass().getName(), leaseParams));
  }

  @Override
  public boolean existsLeasableEntity(DagActionStore.LeaseParams leaseParams) throws IOException {
    return decoratedMultiActiveLeaseArbiter.existsLeasableEntity(leaseParams);
  }

  @Override
  public boolean recordLeaseSuccess(LeaseAttemptStatus.LeaseObtainedStatus status)
      throws IOException {
    if (this.decoratedMultiActiveLeaseArbiter.recordLeaseSuccess(status)) {
      this.recordedLeaseSuccessCount.mark();
      return true;
    }
    this.failedToRecordLeaseSuccessCount.mark();
    return false;
  }
}
