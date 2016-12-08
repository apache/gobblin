/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.metrics.performance;


import org.testng.annotations.Test;

import com.google.common.collect.Sets;


/**
 * Class for running Gobblin metrics performance tests.
 */
@Test(groups = { "performance" })
public class MetricsPerformanceTest {

  public void counterPerformance() throws Exception {
    PerformanceUtils.multiTest().name("CounterPerformance").threads(1).depth(Sets.newHashSet(0, 1, 2, 3)).
        forkAtDepth(0).counters(Sets.newHashSet(0, 1, 2, 3)).run();
  }

  public void meterPerformance() throws Exception {
    PerformanceUtils.multiTest().name("MeterPerformance").threads(1).depth(Sets.newHashSet(0, 1, 2, 3)).
        forkAtDepth(0).meters(Sets.newHashSet(0, 1, 2, 3)).queries(20000000l).run();
  }

  public void histogramPerformance() throws Exception {
    PerformanceUtils.multiTest().name("HistogramPerformance").threads(1).depth(Sets.newHashSet(0, 1, 2, 3)).
        forkAtDepth(0).histograms(Sets.newHashSet(0, 1, 2, 3)).queries(10000000l).run();
  }

  public void timerPerformance() throws Exception {
    PerformanceUtils.multiTest().name("TimerPerformance").threads(1).depth(Sets.newHashSet(0, 1, 2, 3)).
        forkAtDepth(0).timers(Sets.newHashSet(0, 1, 2, 3)).queries(10000000l).run();
  }

  public void parallelizationTest() throws Exception {
    PerformanceUtils.multiTest().name("ParallelizationTest").threads(Sets.newHashSet(1, 2, 3, 4, 5, 6, 7, 8)).
        forkAtDepth(Sets.newHashSet(0, 3)).depth(4).counters(1).run();
  }

  public void forkLevelPerformance() throws Exception {
    PerformanceUtils.multiTest().name("ForkLevelPerformance").threads(8).depth(4).counters(1).
        forkAtDepth(Sets.newHashSet(0, 1, 2, 3)).run();
  }

}
