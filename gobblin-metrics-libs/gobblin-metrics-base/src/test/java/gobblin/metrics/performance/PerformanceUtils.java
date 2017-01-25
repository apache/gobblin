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

package gobblin.metrics.performance;

import lombok.Builder;
import lombok.Singular;
import lombok.extern.slf4j.Slf4j;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;


/**
 * Methods to run performance tests on Gobblin Metrics.
 */
@Slf4j
public class PerformanceUtils {

  /**
   * Runs a set of performance tests. The method will take the cardinal product of the values of each input parameter,
   * and run a performance test for each combination of paramters. At the end, it will print out the results.
   *
   * <p>
   *   All parameters (except for queries) are a set of integers, meaning that separate tests will be run for all
   *   the values provided. The number of queries will be identical for all tests.
   * </p>
   *
   * @param threads Number of threads to spawn. Each thread will have an {@link Incrementer} and update metrics.
   * @param depth Depth of the {@link gobblin.metrics.MetricContext} tree.
   * @param forkAtDepth If multiple threads, each thread has its own {@link gobblin.metrics.MetricContext}. This
   *                    parameter sets the first level in the tree where the per-thread MetricContexts branch off.
   * @param counters Number of counters to generate per thread.
   * @param meters Number of meters to generate per thread.
   * @param histograms Number of histograms to generate per thread.
   * @param timers Number of timers to generate per thread.
   * @param queries Number of increments to do, divided among all threads.
   * @throws Exception
   */
  @Builder(buildMethodName = "run", builderMethodName = "multiTest")
  public static void _multiTest(@Singular("threads") Set<Integer> threads,
      @Singular("depth") Set<Integer> depth,
      @Singular("forkAtDepth") Set<Integer> forkAtDepth,
      @Singular("counters") Set<Integer> counters,
      @Singular("meters") Set<Integer> meters,
      @Singular("histograms") Set<Integer> histograms,
      @Singular("timers") Set<Integer> timers,
      long queries, String name) throws Exception {

    if(threads.isEmpty()) {
      threads = Sets.newHashSet(1);
    }
    if(forkAtDepth.isEmpty()) {
      forkAtDepth = Sets.newHashSet(0);
    }
    if(depth.isEmpty()) {
      depth = Sets.newHashSet(0);
    }
    if(counters.isEmpty()) {
      counters = Sets.newHashSet(0);
    }
    if(meters.isEmpty()) {
      meters = Sets.newHashSet(0);
    }
    if(histograms.isEmpty()) {
      histograms = Sets.newHashSet(0);
    }
    if(timers.isEmpty()) {
      timers = Sets.newHashSet(0);
    }
    if(queries == 0) {
      queries = 50000000l;
    }
    if(Strings.isNullOrEmpty(name)) {
      name = "Test";
    }

    Set<List<Integer>> parameters = Sets.cartesianProduct(threads, depth, forkAtDepth, counters, meters,
        histograms, timers);

    Comparator<List<Integer>> comparator = new Comparator<List<Integer>>() {
      @Override public int compare(List<Integer> o1, List<Integer> o2) {
        Iterator<Integer> it1 = o1.iterator();
        Iterator<Integer> it2 = o2.iterator();

        while(it1.hasNext() && it2.hasNext()) {
          int compare = Integer.compare(it1.next(), it2.next());
          if(compare != 0) {
            return compare;
          }
        }
        if(it1.hasNext()) {
          return 1;
        } else if(it2.hasNext()) {
          return -1;
        } else {
          return 0;
        }
      }
    };

    TreeMap<List<Integer>, Double> results = Maps.newTreeMap(comparator);

    for(List<Integer> p : parameters) {
      Preconditions.checkArgument(p.size() == 7, "Parameter list should be of size 7.");
      results.put(p, singleTest().threads(p.get(0)).depth(p.get(1)).forkAtDepth(p.get(2)).counters(p.get(3)).
          meters(p.get(4)).histograms(p.get(5)).timers(p.get(6)).queries(queries).run());
    }

    System.out.println("===========================");
    System.out.println(name);
    System.out.println("===========================");
    System.out.println("Threads\tDepth\tForkAtDepth\tCounters\tMeters\tHistograms\tTimers\tQPS");
    for(Map.Entry<List<Integer>, Double> result : results.entrySet()) {
      List<Integer> p = result.getKey();
      System.out.println(String
          .format("%d\t%d\t%d\t%d\t%d\t%d\t%d\t%f", p.get(0), p.get(1), p.get(2), p.get(3), p.get(4), p.get(5),
              p.get(6), result.getValue()));
    }
  }

  /**
   * Runs a single performance test. Creates a {@link gobblin.metrics.MetricContext} tree, spawns a number of threads,
   * uses and {@link Incrementer} to update the metrics repeatedly, then determines the achieved QPS in number
   * of iterations of {@link Incrementer} per second.
   *
   * @param threads Number of threads to spawn. Each thread will have an {@link Incrementer} and update metrics.
   * @param depth Depth of the {@link gobblin.metrics.MetricContext} tree.
   * @param forkAtDepth If multiple threads, each thread has its own {@link gobblin.metrics.MetricContext}. This
   *                    parameter sets the first level in the tree where the per-thread MetricContexts branch off.
   * @param counters Number of counters to generate per thread.
   * @param meters Number of meters to generate per thread.
   * @param histograms Number of histograms to generate per thread.
   * @param timers Number of timers to generate per thread.
   * @param queries Number of increments to do, divided among all threads.
   * @return total QPS achieved (e.g. total increments per second in the {@link Incrementer}s)
   * @throws Exception
   */
  @Builder(buildMethodName = "run", builderMethodName = "singleTest")
  public static double _singleTest(int threads, int depth, int forkAtDepth, int counters, int meters, int histograms,
      int timers, long queries) throws Exception {

    System.gc();

    ExecutorService executorService = Executors.newFixedThreadPool(threads);
    if(queries == 0) {
      queries = 50000000l;
    }
    long queriesPerThread = queries / threads;
    long actualQueries = queriesPerThread * threads;

    MetricsUpdater commonUpdater = MetricsUpdater.builder().depth(forkAtDepth).build();

    List<Incrementer> incrementerList = Lists.newArrayList();
    while(incrementerList.size() < threads) {
      final MetricsUpdater metricsUpdater = MetricsUpdater.builder().baseContext(commonUpdater.getContext()).
          depth(depth - forkAtDepth).counters(counters).meters(meters).histograms(histograms).timers(timers).build();
      incrementerList.add(new Incrementer(queriesPerThread, new Runnable() {
        @Override public void run() {
          metricsUpdater.run();
        }
      }));
    }

    List<Future<Long>> incrementerFutures = Lists.newArrayList();
    for(Incrementer incrementer : incrementerList) {
      incrementerFutures.add(executorService.submit(incrementer));
    }

    long totalTime = 0;
    for(Future<Long> future : incrementerFutures) {
      totalTime += future.get();
    }
    double averageTime = (double) totalTime / threads;

    double qps = 1000 * (double)actualQueries / averageTime;
    log.info(String.format("Average qps: %f.", qps));

    executorService.shutdown();

    return qps;
  }

}
