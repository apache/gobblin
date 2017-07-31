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

package org.apache.gobblin.source.workunit;

import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Longs;


/**
 * Helper class that distributes {@link WorkUnit}s amongst a series of {@link MultiWorkUnit}s. When a WorkUnit is added
 * to this queue, it is added along with a weight which indicates how much effort it will take to process this WorkUnit.
 * For example, a larger weight means that this WorkUnit will take longer to process. For files this can simply be the
 * file size.
 *
 * <p>
 *
 * The constructor {@link MultiWorkUnitWeightedQueue(int maxMultiWorkUnits)} sets a maximum size for the queue. This
 * means that when more than maxMultiWorkUnits are added to the queue, WorkUnits will start to be paired together into
 * MultiWorkUnits.
 *
 * @see MultiWorkUnit
 */
public class MultiWorkUnitWeightedQueue {

  private final Queue<WeightedMultiWorkUnit> weightedWorkUnitQueue;

  private int maxMultiWorkUnits = Integer.MAX_VALUE;
  private int numMultiWorkUnits = 0;

  /**
   * The default constructor sets the limit on the queue to size to be {@link Integer#MAX_VALUE}. This means that until
   * Integer.MAX_VALUE + 1 WorkUnits are added to the queue, no WorkUnits will be paired together.
   */
  public MultiWorkUnitWeightedQueue() {
    this.weightedWorkUnitQueue = new PriorityQueue<>();
  }

  public MultiWorkUnitWeightedQueue(int maxMultiWorkUnits) {
    this.weightedWorkUnitQueue = new PriorityQueue<>(maxMultiWorkUnits);
    this.maxMultiWorkUnits = maxMultiWorkUnits;
  }

  /**
   * Adds a {@link WorkUnit} to this queue, along with an associated weight for that WorkUnit.
   */
  public void addWorkUnit(WorkUnit workUnit, long weight) {

    WeightedMultiWorkUnit weightMultiWorkUnit;

    if (this.numMultiWorkUnits < this.maxMultiWorkUnits) {
      weightMultiWorkUnit = new WeightedMultiWorkUnit();
      this.numMultiWorkUnits++;

    } else {

      weightMultiWorkUnit = this.weightedWorkUnitQueue.poll();
    }

    weightMultiWorkUnit.addWorkUnit(weight, workUnit);
    this.weightedWorkUnitQueue.offer(weightMultiWorkUnit);

  }

  /**
   * Returns the a list of WorkUnits that have been added to this queue via the {@link #addWorkUnit(WorkUnit, long)}
   * method.
   */
  public List<WorkUnit> getQueueAsList() {
    return ImmutableList.<WorkUnit> builder().addAll(this.weightedWorkUnitQueue).build();
  }

  /**
   * This class defines the weighted multiWorkUnit. It extends {@link MultiWorkUnit}. Each weightedMultiworkUnit has a
   * weight, which is the sum of the file sizes assigned to it. It also implements Comparable, based on the weight value.
   *
   * @author ydai
   */
  private static class WeightedMultiWorkUnit extends MultiWorkUnit implements Comparable<WeightedMultiWorkUnit> {

    private long weight = 0l;

    /**
     * Add a new single workUnit to the current workUnits list. Update the weight by adding the weight of the new workUnit.
     *
     * @param weight the weight of the newWorkUnit.
     * @param newWorkUnit the new work unit.
     */
    private void addWorkUnit(long weight, WorkUnit newWorkUnit) {
      this.addWorkUnit(newWorkUnit);
      this.weight += weight;
    }

    /**
     * Compare with the other weightedMultiWorkUnit based on weight.
     */
    @Override
    public int compareTo(WeightedMultiWorkUnit weightedMultiWorkUnit) {
      return Longs.compare(this.weight, weightedMultiWorkUnit.getWeight());
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + (int) (this.weight ^ (this.weight >>> 32));
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof WeightedMultiWorkUnit)) {
        return false;
      }
      WeightedMultiWorkUnit weightedMultiWorkUnit = (WeightedMultiWorkUnit) obj;
      return this.weight == weightedMultiWorkUnit.getWeight();
    }

    public long getWeight() {
      return this.weight;
    }
  }
}
