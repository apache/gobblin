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

package gobblin.util.binpacking;

import java.io.Serializable;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.google.common.base.Predicate;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.MinMaxPriorityQueue;

import gobblin.source.workunit.MultiWorkUnit;
import gobblin.source.workunit.WorkUnit;
import gobblin.source.workunit.WorkUnitBinPacker;
import gobblin.source.workunit.WorkUnitWeighter;

import javax.annotation.Nullable;
import javax.annotation.OverridingMethodsMustInvokeSuper;
import lombok.AllArgsConstructor;


/**
 * Implements a bin packing algorithm similar to worst-fit decreasing bin packing. Given a input list of {@link WorkUnit}s,
 * a {@link WorkUnitWeighter} function, and a maximum weight per {@link MultiWorkUnit}, packs the input work units into
 * {@link MultiWorkUnit}s.
 *
 * <p>
 *   The algorithm is as follows:
 *   * Sort work units decreasing by weight.
 *   * Compute the minimum number of {@link MultiWorkUnit}s needed, and create them.
 *   * For each work unit, find the {@link MultiWorkUnit} with the largest space available, if it fits, add it there,
 *     otherwise, create a new {@link MultiWorkUnit} and add the work unit there.
 * </p>
 */
@AllArgsConstructor
public class WorstFitDecreasingBinPacking implements WorkUnitBinPacker {

  public static final String TOTAL_MULTI_WORK_UNIT_WEIGHT = "binpacking.multiWorkUnit.totalWeight";

  private final long maxWeightPerUnit;

  @Override
  @OverridingMethodsMustInvokeSuper
  public List<WorkUnit> pack(List<WorkUnit> workUnitsIn, WorkUnitWeighter weighter) {

    if (this.maxWeightPerUnit <= 0) { // just return the input
      return workUnitsIn;
    }

    List<WorkUnit> workUnits = Lists.newArrayList(workUnitsIn);

    long smallUnitSize = 0; // total size of work units smaller than maxWeightPerUnit
    int largeUnits = 0; // number of work units larger than maxWeightPerUnit
    for (WorkUnit workUnit : workUnits) {
      long weight = weighter.weight(workUnit);
      if (weight <= this.maxWeightPerUnit) {
        smallUnitSize += weight;
      } else {
        largeUnits++;
      }
    }
    int estimateByWeight = largeUnits + (int) ((smallUnitSize - 1) / this.maxWeightPerUnit) + 1;
    int estimatedMultiWorkUnits = Math.min(estimateByWeight, workUnits.size());

    MinMaxPriorityQueue<MultiWorkUnit> pQueue = MinMaxPriorityQueue.orderedBy(new MultiWorkUnitComparator()).create();
    for (int i = 0; i < estimatedMultiWorkUnits; i++) {
      pQueue.add(MultiWorkUnit.createEmpty());
    }

    Collections.sort(workUnits, Collections.reverseOrder(new WeightComparator(weighter)));

    for (WorkUnit workUnit : workUnits) {
      MultiWorkUnit lightestMultiWorkUnit = pQueue.peek();
      long weight = Math.max(1, weighter.weight(workUnit));
      long multiWorkUnitWeight = getMultiWorkUnitWeight(lightestMultiWorkUnit);
      if (multiWorkUnitWeight == 0 || (weight + multiWorkUnitWeight <= this.maxWeightPerUnit
          && weight + multiWorkUnitWeight > multiWorkUnitWeight)) { // check for overflow
        // if it fits, add it to lightest work unit
        addToMultiWorkUnit(lightestMultiWorkUnit, workUnit, weight);
        pQueue.poll();
        pQueue.add(lightestMultiWorkUnit);
      } else {
        // if doesn't fit in lightest multi work unit, create a new work unit for it
        MultiWorkUnit newMultiWorkUnit = MultiWorkUnit.createEmpty();
        addToMultiWorkUnit(newMultiWorkUnit, workUnit, weight);
        pQueue.add(newMultiWorkUnit);
      }
    }

    return Lists.<WorkUnit> newArrayList(Iterables.filter(pQueue, new Predicate<MultiWorkUnit>() {
      @Override
      public boolean apply(@Nullable MultiWorkUnit input) {
        return getMultiWorkUnitWeight(input) > 0;
      }
    }));
  }

  private static class WeightComparator implements Comparator<WorkUnit> {

    private final WorkUnitWeighter weighter;
    private final LoadingCache<WorkUnit, Long> weightCache;

    public WeightComparator(final WorkUnitWeighter weighter) {
      this.weighter = weighter;
      this.weightCache = CacheBuilder.newBuilder().softValues().build(new CacheLoader<WorkUnit, Long>() {
        @Override
        public Long load(WorkUnit key) throws Exception {
          return WeightComparator.this.weighter.weight(key);
        }
      });
    }

    @Override
    public int compare(WorkUnit o1, WorkUnit o2) {
      try {
        return Long.compare(this.weightCache.get(o1), this.weightCache.get(o2));
      } catch (ExecutionException ee) {
        throw new RuntimeException(ee);
      }
    }
  }

  private static void addToMultiWorkUnit(MultiWorkUnit multiWorkUnit, WorkUnit workUnit, long weight) {
    multiWorkUnit.addWorkUnit(workUnit);
    setMultiWorkUnitWeight(multiWorkUnit, getMultiWorkUnitWeight(multiWorkUnit) + weight);
  }

  private static class MultiWorkUnitComparator implements Comparator<MultiWorkUnit>, Serializable {
    private static final long serialVersionUID = 1L;

    @Override
    public int compare(MultiWorkUnit o1, MultiWorkUnit o2) {
      return Long.compare(getMultiWorkUnitWeight(o1), getMultiWorkUnitWeight(o2));
    }
  }

  private static long getMultiWorkUnitWeight(MultiWorkUnit multiWorkUnit) {
    return multiWorkUnit.contains(TOTAL_MULTI_WORK_UNIT_WEIGHT)
        ? multiWorkUnit.getPropAsLong(TOTAL_MULTI_WORK_UNIT_WEIGHT) : 0;
  }

  private static void setMultiWorkUnitWeight(MultiWorkUnit multiWorkUnit, long weight) {
    multiWorkUnit.setProp(TOTAL_MULTI_WORK_UNIT_WEIGHT, Long.toString(weight));
  }

}
