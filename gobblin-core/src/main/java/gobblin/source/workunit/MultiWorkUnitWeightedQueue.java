package gobblin.source.workunit;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

import com.google.common.primitives.Longs;

public class MultiWorkUnitWeightedQueue {

  private PriorityQueue<WeightedMultiWorkUnit> weightedWorkUnitQueue;

  private int maxMultiWorkUnits = Integer.MAX_VALUE;
  private int numMultiWorkUnits = 0;

  public MultiWorkUnitWeightedQueue() {
    this.weightedWorkUnitQueue =
        new PriorityQueue<WeightedMultiWorkUnit>();
  }

  public MultiWorkUnitWeightedQueue(int maxMultiWorkUnits) {
    this.weightedWorkUnitQueue =
        new PriorityQueue<WeightedMultiWorkUnit>(maxMultiWorkUnits);
    this.maxMultiWorkUnits = maxMultiWorkUnits;
  }

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

  public List<WorkUnit> getList() {
    return new ArrayList<WorkUnit>(this.weightedWorkUnitQueue);
  }

  /**
   * This class defines the weighted multiWorkUnit. It extends {@link gobblin.source.workunit.MultiWorkUnit}.
   * Each weightedMultiworkUnit has a weight, which is the sum of the file sizes assigned to it.
   * It also implements Comparable, based on the weight value.
   * @author ydai
   */
  private class WeightedMultiWorkUnit extends MultiWorkUnit implements Comparable<WeightedMultiWorkUnit> {

    private long weight = 0l;

    /**
     * Add a new single workUnit to the current workUnits list.
     * Update the weight by adding the weight of the new workUnit.
     *
     * @param weight the weight of the newWorkUnit
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

    public long getWeight() {
      return this.weight;
    }
  }
}
