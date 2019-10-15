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

package org.apache.gobblin.cluster;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.common.util.concurrent.AtomicDouble;
import com.sun.management.OperatingSystemMXBean;
import com.typesafe.config.Config;

import org.apache.gobblin.metrics.ContextAwareGauge;
import org.apache.gobblin.metrics.RootMetricContext;
import org.apache.gobblin.util.ConfigUtils;


/**
 * A utility class that periodically emits system level metrics that report the health of the container.
 * Reported metrics include CPU/Memory usage of the JVM, system load etc.
 *
 * <p>
 *   This class extends the {@link AbstractScheduledService} so it can be used with a
 *   {@link com.google.common.util.concurrent.ServiceManager} that manages the lifecycle of
 *   a {@link ContainerHealthMetricsService}.
 * </p>
 * TODO: Add Garbage Collection metrics.
*/
public class ContainerHealthMetricsService extends AbstractScheduledService {
  //Container metrics service configurations
  private static final String CONTAINER_METRICS_SERVICE_REPORTING_INTERVAL_SECONDS = "container.health.metrics.service.reportingIntervalSeconds";
  private static final Long DEFAULT_CONTAINER_METRICS_REPORTING_INTERVAL = 30L;

  private final long metricReportingInterval;
  private final OperatingSystemMXBean operatingSystemMXBean;
  private final MemoryMXBean memoryMXBean;

  AtomicDouble processCpuLoad = new AtomicDouble(0);
  AtomicDouble systemCpuLoad = new AtomicDouble(0);
  AtomicDouble systemLoadAvg = new AtomicDouble(0);
  AtomicLong committedVmemSize = new AtomicLong(0);
  AtomicLong processCpuTime = new AtomicLong(0);
  AtomicLong freeSwapSpaceSize = new AtomicLong(0);
  AtomicLong numAvailableProcessors = new AtomicLong(0);
  AtomicLong totalPhysicalMemSize = new AtomicLong(0);
  AtomicLong totalSwapSpaceSize = new AtomicLong(0);
  AtomicLong freePhysicalMemSize = new AtomicLong(0);
  AtomicLong processHeapUsedSize = new AtomicLong(0);

  public ContainerHealthMetricsService(Config config) {
    this.metricReportingInterval = ConfigUtils.getLong(config, CONTAINER_METRICS_SERVICE_REPORTING_INTERVAL_SECONDS, DEFAULT_CONTAINER_METRICS_REPORTING_INTERVAL);
    this.operatingSystemMXBean = ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class);
    this.memoryMXBean = ManagementFactory.getMemoryMXBean();

    //Build all the gauges and register them with the metrics registry.
    List<ContextAwareGauge<Double>> systemMetrics = buildGaugeList();
    systemMetrics.forEach(metric -> RootMetricContext.get().register(metric));
  }

  /**
   * Run one iteration of the scheduled task. If any invocation of this method throws an exception,
   * the service will transition to the {@link com.google.common.util.concurrent.Service.State#FAILED} state and this method will no
   * longer be called.
   */
  @Override
  protected void runOneIteration() throws Exception {
    this.processCpuLoad.set(this.operatingSystemMXBean.getProcessCpuLoad());
    this.systemCpuLoad.set(this.operatingSystemMXBean.getSystemCpuLoad());
    this.systemLoadAvg.set(this.operatingSystemMXBean.getSystemLoadAverage());
    this.committedVmemSize.set(this.operatingSystemMXBean.getCommittedVirtualMemorySize());
    this.processCpuTime.set(this.operatingSystemMXBean.getProcessCpuTime());
    this.freeSwapSpaceSize.set(this.operatingSystemMXBean.getFreeSwapSpaceSize());
    this.numAvailableProcessors.set(this.operatingSystemMXBean.getAvailableProcessors());
    this.totalPhysicalMemSize.set(this.operatingSystemMXBean.getTotalPhysicalMemorySize());
    this.totalSwapSpaceSize.set(this.operatingSystemMXBean.getTotalSwapSpaceSize());
    this.freePhysicalMemSize.set(this.operatingSystemMXBean.getFreePhysicalMemorySize());
    this.processHeapUsedSize.set(this.memoryMXBean.getHeapMemoryUsage().getUsed());
  }

  protected List<ContextAwareGauge<Double>> buildGaugeList() {
    List<ContextAwareGauge<Double>> gaugeList = new ArrayList<>();
    gaugeList.add(RootMetricContext.get().newContextAwareGauge(ContainerHealthMetrics.PROCESS_CPU_LOAD,
        () -> this.processCpuLoad.get()));
    gaugeList.add(RootMetricContext.get().newContextAwareGauge(ContainerHealthMetrics.SYSTEM_CPU_LOAD,
        () -> this.systemCpuLoad.get()));
    gaugeList.add(RootMetricContext.get().newContextAwareGauge(ContainerHealthMetrics.SYSTEM_LOAD_AVG,
        () -> this.systemLoadAvg.get()));
    gaugeList.add(RootMetricContext.get().newContextAwareGauge(ContainerHealthMetrics.COMMITTED_VMEM_SIZE,
        () -> Long.valueOf(this.committedVmemSize.get()).doubleValue()));
    gaugeList.add(RootMetricContext.get().newContextAwareGauge(ContainerHealthMetrics.PROCESS_CPU_TIME,
        () -> Long.valueOf(this.processCpuTime.get()).doubleValue()));
    gaugeList.add(RootMetricContext.get().newContextAwareGauge(ContainerHealthMetrics.FREE_SWAP_SPACE_SIZE,
        () -> Long.valueOf(this.freeSwapSpaceSize.get()).doubleValue()));
    gaugeList.add(RootMetricContext.get().newContextAwareGauge(ContainerHealthMetrics.NUM_AVAILABLE_PROCESSORS,
        () -> Long.valueOf(this.numAvailableProcessors.get()).doubleValue()));
    gaugeList.add(RootMetricContext.get().newContextAwareGauge(ContainerHealthMetrics.TOTAL_PHYSICAL_MEM_SIZE,
        () -> Long.valueOf(this.totalPhysicalMemSize.get()).doubleValue()));
    gaugeList.add(RootMetricContext.get().newContextAwareGauge(ContainerHealthMetrics.TOTAL_SWAP_SPACE_SIZE,
        () -> Long.valueOf(this.totalSwapSpaceSize.get()).doubleValue()));
    gaugeList.add(RootMetricContext.get().newContextAwareGauge(ContainerHealthMetrics.FREE_PHYSICAL_MEM_SIZE,
        () -> Long.valueOf(this.freePhysicalMemSize.get()).doubleValue()));
    gaugeList.add(RootMetricContext.get().newContextAwareGauge(ContainerHealthMetrics.PROCESS_HEAP_USED_SIZE,
        () -> Long.valueOf(this.processHeapUsedSize.get()).doubleValue()));
    return gaugeList;
  }

  /**
   * Returns the {@link Scheduler} object used to configure this service.  This method will only be
   * called once.
   */
  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedRateSchedule(0, this.metricReportingInterval, TimeUnit.SECONDS);
  }
}
