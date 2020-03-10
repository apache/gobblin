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

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.common.util.concurrent.AtomicDouble;
import com.sun.management.OperatingSystemMXBean;
import com.typesafe.config.Config;

import lombok.Data;
import lombok.Getter;

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
*/
public class ContainerHealthMetricsService extends AbstractScheduledService {
  //Container metrics service configurations
  private static final String CONTAINER_METRICS_SERVICE_REPORTING_INTERVAL_SECONDS = "container.health.metrics.service.reportingIntervalSeconds";
  private static final Long DEFAULT_CONTAINER_METRICS_REPORTING_INTERVAL = 30L;
  private static final Set<String> YOUNG_GC_TYPES = new HashSet<>(3);
  private static final Set<String> OLD_GC_TYPES = new HashSet<String>(3);

  static {
    // young generation GC names
    YOUNG_GC_TYPES.add("PS Scavenge");
    YOUNG_GC_TYPES.add("ParNew");
    YOUNG_GC_TYPES.add("G1 Young Generation");

    // old generation GC names
    OLD_GC_TYPES.add("PS MarkSweep");
    OLD_GC_TYPES.add("ConcurrentMarkSweep");
    OLD_GC_TYPES.add("G1 Old Generation");
  }

  private final long metricReportingInterval;
  private final OperatingSystemMXBean operatingSystemMXBean;
  private final MemoryMXBean memoryMXBean;
  private final List<GarbageCollectorMXBean> garbageCollectorMXBeans;

  @Getter
  private GcStats lastGcStats;
  @Getter
  private GcStats currentGcStats;

  //Heap stats
  AtomicDouble processCpuLoad = new AtomicDouble(0);
  AtomicDouble systemCpuLoad = new AtomicDouble(0);
  AtomicDouble systemLoadAvg = new AtomicDouble(0);
  AtomicDouble committedVmemSize = new AtomicDouble(0);
  AtomicDouble processCpuTime = new AtomicDouble(0);
  AtomicDouble freeSwapSpaceSize = new AtomicDouble(0);
  AtomicDouble numAvailableProcessors = new AtomicDouble(0);
  AtomicDouble totalPhysicalMemSize = new AtomicDouble(0);
  AtomicDouble totalSwapSpaceSize = new AtomicDouble(0);
  AtomicDouble freePhysicalMemSize = new AtomicDouble(0);
  AtomicDouble processHeapUsedSize = new AtomicDouble(0);

  //GC stats and counters
  AtomicDouble minorGcCount = new AtomicDouble(0);
  AtomicDouble majorGcCount = new AtomicDouble(0);
  AtomicDouble unknownGcCount = new AtomicDouble(0);
  AtomicDouble minorGcDuration = new AtomicDouble(0);
  AtomicDouble majorGcDuration = new AtomicDouble(0);
  AtomicDouble unknownGcDuration = new AtomicDouble(0);

  public ContainerHealthMetricsService(Config config) {
    this.metricReportingInterval = ConfigUtils.getLong(config, CONTAINER_METRICS_SERVICE_REPORTING_INTERVAL_SECONDS, DEFAULT_CONTAINER_METRICS_REPORTING_INTERVAL);
    this.operatingSystemMXBean = ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class);
    this.memoryMXBean = ManagementFactory.getMemoryMXBean();
    this.garbageCollectorMXBeans = ManagementFactory.getGarbageCollectorMXBeans();
    this.lastGcStats = new GcStats();
    this.currentGcStats = new GcStats();

    //Build all the gauges and register them with the metrics registry.
    List<ContextAwareGauge<Double>> systemMetrics = buildGaugeList();
    systemMetrics.forEach(metric -> RootMetricContext.get().register(metric));
  }

  @Data
  public static class GcStats {
    long minorCount;
    double minorDuration;
    long majorCount;
    double majorDuration;
    long unknownCount;
    double unknownDuration;
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

    //Get the new GC stats
    this.currentGcStats = collectGcStats();
    // Since GC Beans report accumulated counts/durations, we need to subtract the previous values to obtain the counts/durations
    // since the last measurement time.
    this.minorGcCount.set(this.currentGcStats.getMinorCount() - this.lastGcStats.getMinorCount());
    this.minorGcDuration.set(this.currentGcStats.getMinorDuration() - this.lastGcStats.getMinorDuration());
    this.majorGcCount.set(this.currentGcStats.getMajorCount() - this.lastGcStats.getMajorCount());
    this.majorGcDuration.set(this.currentGcStats.getMajorDuration() - this.lastGcStats.getMajorDuration());
    this.unknownGcCount.set(this.currentGcStats.getUnknownCount() - this.lastGcStats.getUnknownCount());
    this.unknownGcDuration.set(this.currentGcStats.getUnknownDuration() - this.lastGcStats.getUnknownDuration());

    //Update last collected stats
    this.lastGcStats = this.currentGcStats;
  }

  protected List<ContextAwareGauge<Double>> buildGaugeList() {
    List<ContextAwareGauge<Double>> gaugeList = new ArrayList<>();

    gaugeList.add(createGauge(ContainerHealthMetrics.PROCESS_CPU_LOAD, this.processCpuLoad));
    gaugeList.add(createGauge(ContainerHealthMetrics.SYSTEM_CPU_LOAD, this.systemCpuLoad));
    gaugeList.add(createGauge(ContainerHealthMetrics.SYSTEM_LOAD_AVG, this.systemLoadAvg));
    gaugeList.add(createGauge(ContainerHealthMetrics.COMMITTED_VMEM_SIZE, this.committedVmemSize));
    gaugeList.add(createGauge(ContainerHealthMetrics.PROCESS_CPU_TIME, this.processCpuTime));
    gaugeList.add(createGauge(ContainerHealthMetrics.FREE_SWAP_SPACE_SIZE, this.freeSwapSpaceSize));
    gaugeList.add(createGauge(ContainerHealthMetrics.NUM_AVAILABLE_PROCESSORS, this.numAvailableProcessors));
    gaugeList.add(createGauge(ContainerHealthMetrics.TOTAL_PHYSICAL_MEM_SIZE, this.totalPhysicalMemSize));
    gaugeList.add(createGauge(ContainerHealthMetrics.TOTAL_SWAP_SPACE_SIZE, this.totalSwapSpaceSize));
    gaugeList.add(createGauge(ContainerHealthMetrics.FREE_PHYSICAL_MEM_SIZE, this.freePhysicalMemSize));
    gaugeList.add(createGauge(ContainerHealthMetrics.PROCESS_HEAP_USED_SIZE, this.processHeapUsedSize));
    gaugeList.add(createGauge(ContainerHealthMetrics.MINOR_GC_COUNT, this.minorGcCount));
    gaugeList.add(createGauge(ContainerHealthMetrics.MINOR_GC_DURATION, this.minorGcDuration));
    gaugeList.add(createGauge(ContainerHealthMetrics.MAJOR_GC_COUNT, this.majorGcCount));
    gaugeList.add(createGauge(ContainerHealthMetrics.MAJOR_GC_DURATION, this.majorGcDuration));
    gaugeList.add(createGauge(ContainerHealthMetrics.UNKNOWN_GC_COUNT, this.unknownGcCount));
    gaugeList.add(createGauge(ContainerHealthMetrics.UNKNOWN_GC_DURATION, this.unknownGcDuration));
    return gaugeList;
  }

  private ContextAwareGauge<Double> createGauge(String name, AtomicDouble metric) {
    return RootMetricContext.get().newContextAwareGauge(name, () -> metric.get());
  }

  private GcStats collectGcStats() {
    //Collect GC stats by iterating over all GC beans.
    GcStats gcStats = new GcStats();

    for (GarbageCollectorMXBean garbageCollectorMXBean: this.garbageCollectorMXBeans) {
      long count = garbageCollectorMXBean.getCollectionCount();
      double duration = (double) garbageCollectorMXBean.getCollectionTime();
      if (count >= 0) {
        if (YOUNG_GC_TYPES.contains(garbageCollectorMXBean.getName())) {
          gcStats.setMinorCount(gcStats.getMinorCount() + count);
          gcStats.setMinorDuration(gcStats.getMinorDuration() + duration);
        }
        else if (OLD_GC_TYPES.contains(garbageCollectorMXBean.getName())) {
          gcStats.setMajorCount(gcStats.getMajorCount() + count);
          gcStats.setMajorDuration(gcStats.getMajorDuration() + duration);
        } else {
          gcStats.setUnknownCount(gcStats.getUnknownCount() + count);
          gcStats.setUnknownDuration(gcStats.getUnknownDuration() + duration);
        }
      }
    }
    return gcStats;
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
