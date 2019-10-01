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

public class ContainerHealthMetrics {
  public static final String CONTAINER_METRICS_PREFIX = "container.health.metrics.";

  public static final String PROCESS_CPU_LOAD = CONTAINER_METRICS_PREFIX + "processCpuLoad";
  public static final String PROCESS_CPU_TIME = CONTAINER_METRICS_PREFIX + "processCpuTime";
  public static final String PROCESS_HEAP_USED_SIZE = CONTAINER_METRICS_PREFIX + "processHeapUsedSize";
  public static final String SYSTEM_CPU_LOAD = CONTAINER_METRICS_PREFIX + "systemCpuLoad";
  public static final String SYSTEM_LOAD_AVG = CONTAINER_METRICS_PREFIX + "systemLoadAvg";
  public static final String COMMITTED_VMEM_SIZE = CONTAINER_METRICS_PREFIX + "committedVmemSize";
  public static final String FREE_SWAP_SPACE_SIZE = CONTAINER_METRICS_PREFIX + "freeSwapSpaceSize";
  public static final String TOTAL_SWAP_SPACE_SIZE = CONTAINER_METRICS_PREFIX + "totalSwapSpaceSize";
  public static final String NUM_AVAILABLE_PROCESSORS = CONTAINER_METRICS_PREFIX + "numAvailableProcessors";
  public static final String TOTAL_PHYSICAL_MEM_SIZE = CONTAINER_METRICS_PREFIX + "totalPhysicalMemSize";
  public static final String FREE_PHYSICAL_MEM_SIZE = CONTAINER_METRICS_PREFIX + "freePhysicalMemSize";

}
