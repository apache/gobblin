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

package gobblin.source.extractor.extract.kafka.workunit.packer;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import com.google.common.collect.Lists;

import gobblin.configuration.SourceState;
import gobblin.configuration.State;
import gobblin.source.extractor.extract.AbstractSource;
import gobblin.source.workunit.MultiWorkUnit;
import gobblin.source.workunit.WorkUnit;


/**
 * An implementation of {@link KafkaWorkUnitPacker} with two levels of bin packing.
 *
 * In the first level, some {@link WorkUnit}s corresponding to partitions
 * of the same topic are grouped together into a single {@link WorkUnit}. The number of grouped {@link WorkUnit}s
 * is approximately {@link #WORKUNIT_PRE_GROUPING_SIZE_FACTOR} * number of {@link MultiWorkUnit}s. The value of
 * {@link #WORKUNIT_PRE_GROUPING_SIZE_FACTOR} should generally be 3.0 or higher, since the worst-fit-decreasing
 * algorithm (used by the second level) may not achieve a good balance if the number of items
 * is less than 3 times the number of bins.
 *
 * In the second level, these grouped {@link WorkUnit}s are assembled into {@link MultiWorkunit}s
 * using worst-fit-decreasing.
 *
 * Bi-level bin packing has two advantages: (1) reduce the number of small output files since it tends to pack
 * partitions of the same topic together; (2) reduce the total number of workunits / tasks since multiple partitions
 * of the same topic are assigned to the same task. A task has a non-trivial cost of initialization, tear down and
 * task state persistence. However, bi-level bin packing has more mapper skew than single-level bin packing, because
 * if we pack lots of partitions of the same topic to the same mapper, and we underestimate the avg time per record
 * for this topic, then this mapper could be much slower than other mappers.
 *
 * @author Ziyang Liu
 */
public class KafkaBiLevelWorkUnitPacker extends KafkaWorkUnitPacker {

  public static final String WORKUNIT_PRE_GROUPING_SIZE_FACTOR = "workunit.pre.grouping.size.factor";
  public static final double DEFAULT_WORKUNIT_PRE_GROUPING_SIZE_FACTOR = 3.0;

  protected KafkaBiLevelWorkUnitPacker(AbstractSource<?, ?> source, SourceState state) {
    super(source, state);
  }

  @Override
  public List<WorkUnit> pack(Map<String, List<WorkUnit>> workUnitsByTopic, int numContainers) {
    double totalEstDataSize = setWorkUnitEstSizes(workUnitsByTopic);
    double avgGroupSize = totalEstDataSize / numContainers / getPreGroupingSizeFactor(this.state);

    List<MultiWorkUnit> mwuGroups = Lists.newArrayList();
    for (List<WorkUnit> workUnitsForTopic : workUnitsByTopic.values()) {
      double estimatedDataSizeForTopic = calcTotalEstSizeForTopic(workUnitsForTopic);
      if (estimatedDataSizeForTopic < avgGroupSize) {

        // If the total estimated size of a topic is smaller than group size, put all partitions of this
        // topic in a single group.
        MultiWorkUnit mwuGroup = MultiWorkUnit.createEmpty();
        addWorkUnitsToMultiWorkUnit(workUnitsForTopic, mwuGroup);
        mwuGroups.add(mwuGroup);
      } else {

        // Use best-fit-decreasing to group workunits for a topic into multiple groups.
        mwuGroups.addAll(bestFitDecreasingBinPacking(workUnitsForTopic, avgGroupSize));
      }
    }

    List<WorkUnit> groups = squeezeMultiWorkUnits(mwuGroups);
    return worstFitDecreasingBinPacking(groups, numContainers);
  }

  private static double calcTotalEstSizeForTopic(List<WorkUnit> workUnitsForTopic) {
    double totalSize = 0;
    for (WorkUnit w : workUnitsForTopic) {
      totalSize += getWorkUnitEstSize(w);
    }
    return totalSize;
  }

  private static double getPreGroupingSizeFactor(State state) {
    return state.getPropAsDouble(WORKUNIT_PRE_GROUPING_SIZE_FACTOR, DEFAULT_WORKUNIT_PRE_GROUPING_SIZE_FACTOR);
  }

  /**
   * Group {@link WorkUnit}s into groups. Each group is a {@link MultiWorkUnit}. Each group has a capacity of
   * avgGroupSize. If there's a single {@link WorkUnit} whose size is larger than avgGroupSize, it forms a group itself.
   */
  private static List<MultiWorkUnit> bestFitDecreasingBinPacking(List<WorkUnit> workUnits, double avgGroupSize) {

    // Sort workunits by data size desc
    Collections.sort(workUnits, LOAD_DESC_COMPARATOR);

    PriorityQueue<MultiWorkUnit> pQueue = new PriorityQueue<>(workUnits.size(), LOAD_DESC_COMPARATOR);
    for (WorkUnit workUnit : workUnits) {
      MultiWorkUnit bestGroup = findAndPopBestFitGroup(workUnit, pQueue, avgGroupSize);
      if (bestGroup != null) {
        addWorkUnitToMultiWorkUnit(workUnit, bestGroup);
      } else {
        bestGroup = MultiWorkUnit.createEmpty();
        addWorkUnitToMultiWorkUnit(workUnit, bestGroup);
      }
      pQueue.add(bestGroup);
    }
    return Lists.newArrayList(pQueue);
  }

  /**
   * Find the best group using the best-fit-decreasing algorithm.
   * The best group is the fullest group that has enough capacity for the new {@link WorkUnit}.
   * If no existing group has enough capacity for the new {@link WorkUnit}, return null.
   */
  private static MultiWorkUnit findAndPopBestFitGroup(WorkUnit workUnit, PriorityQueue<MultiWorkUnit> pQueue,
      double avgGroupSize) {

    List<MultiWorkUnit> fullWorkUnits = Lists.newArrayList();
    MultiWorkUnit bestFit = null;

    while (!pQueue.isEmpty()) {
      MultiWorkUnit candidate = pQueue.poll();
      if (getWorkUnitEstSize(candidate) + getWorkUnitEstSize(workUnit) <= avgGroupSize) {
        bestFit = candidate;
        break;
      }
      fullWorkUnits.add(candidate);
    }

    for (MultiWorkUnit fullWorkUnit : fullWorkUnits) {
      pQueue.add(fullWorkUnit);
    }

    return bestFit;
  }

}
