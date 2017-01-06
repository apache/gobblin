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

package gobblin.data.management.copy;

import java.util.Map;

import lombok.Builder;
import lombok.Getter;

import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import gobblin.data.management.partition.FileSet;
import gobblin.source.workunit.WorkUnit;
import lombok.extern.slf4j.Slf4j;


/**
 * A {@link WorkUnit} container that is bounded, supports concurrent all-or-nothing addAll, and supports priority of
 * file sets, ie. attempting to add a file set with higher priority will automatically evict
 * lower priority {@link gobblin.data.management.partition.FileSet}s if necessary.
 *
 * <p>
 *   File sets in {@link CopySource} are handled as {@link gobblin.data.management.partition.FileSet}, so this class uses a {@link gobblin.data.management.partition.FileSet} comparator
 *   for priority. If fileSetA < fileSetB, then fileSetA has higher priority than fileSetB
 *   (similar to {@link java.util.PriorityQueue}).
 * </p>
 */
@Slf4j
class ConcurrentBoundedWorkUnitList {

  private final TreeMap<FileSet<CopyEntity>, List<WorkUnit>> workUnitsMap;
  @Getter
  private final Comparator<FileSet<CopyEntity>> comparator;
  private final int maxSize;
  private final int strictMaxSize;
  private int currentSize;
  /** Set to true the first time a file set is rejected (i.e. doesn't fit in the container) */
  private boolean rejectedFileSet;

  private static class AugmentedComparator implements Comparator<FileSet<CopyEntity>> {
    private final Comparator<FileSet<CopyEntity>> userProvidedComparator;

    public AugmentedComparator(Comparator<FileSet<CopyEntity>> userProvidedComparator) {
      this.userProvidedComparator = userProvidedComparator;
    }

    @Override
    public int compare(FileSet<CopyEntity> p1, FileSet<CopyEntity> p2) {
      int userProvidedCompare = this.userProvidedComparator.compare(p1, p2);
      if (userProvidedCompare == 0) {
        int datasetCompare = p1.getDataset().datasetURN().compareTo(p2.getDataset().datasetURN());
        if (datasetCompare == 0) {
          return p1.getName().compareTo(p2.getName());
        }
        return datasetCompare;
      }
      return userProvidedCompare;
    }
  }

  /**
   * Creates a new {@link ConcurrentBoundedWorkUnitList}.
   * @param maxSize Maximum number of {@link WorkUnit}s to contain.
   * @param comparator {@link Comparator} for {@link gobblin.data.management.partition.FileSet}s to use for {@link gobblin.data.management.partition.FileSet} priority.
   * @param strictLimitMultiplier the list will only start rejecting {@link WorkUnit}s if its capacity exceeds
   *                              maxSize * strictLimitMultiplier. If this parameter is < 1, it will be auto-set to 1.
   */
  @Builder
  public ConcurrentBoundedWorkUnitList(int maxSize, final Comparator<FileSet<CopyEntity>> comparator,
      double strictLimitMultiplier) {
    this.currentSize = 0;
    this.maxSize = maxSize;
    double actualStrictLimitMultiplier =
        Math.min((Integer.MAX_VALUE / (double) this.maxSize), Math.max(1.0, strictLimitMultiplier));
    this.strictMaxSize = (int) (this.maxSize * actualStrictLimitMultiplier);
    this.comparator = comparator == null ? new AllEqualComparator<FileSet<CopyEntity>>() : comparator;
    this.workUnitsMap = new TreeMap<>(new AugmentedComparator(this.comparator));
    this.rejectedFileSet = false;
  }

  /**
   * Add a file set to the container.
   * @param fileSet File set, expressed as a {@link gobblin.data.management.partition.FileSet} of {@link CopyEntity}s.
   * @param workUnits List of {@link WorkUnit}s corresponding to this file set.
   * @return true if the file set was added to the container, false otherwise (i.e. has reached max size).
   */
  public boolean addFileSet(FileSet<CopyEntity> fileSet, List<WorkUnit> workUnits) {
    boolean addedWorkunits = addFileSetImpl(fileSet, workUnits);
    if (!addedWorkunits) {
      this.rejectedFileSet = true;
    }
    return addedWorkunits;
  }

  private synchronized boolean addFileSetImpl(FileSet<CopyEntity> fileSet, List<WorkUnit> workUnits) {
    if (this.currentSize + workUnits.size() > this.strictMaxSize) {
      if (this.comparator.compare(this.workUnitsMap.lastKey(), fileSet) <= 0) {
        return false;
      }
      int tmpSize = this.currentSize;
      Set<FileSet<CopyEntity>> partitionsToDelete = Sets.newHashSet();

      for (FileSet<CopyEntity> existingFileSet : this.workUnitsMap.descendingKeySet()) {
        if (this.comparator.compare(existingFileSet, fileSet) <= 0) {
          return false;
        }
        tmpSize -= this.workUnitsMap.get(existingFileSet).size();
        partitionsToDelete.add(existingFileSet);
        if (tmpSize + workUnits.size() <= this.strictMaxSize) {
          break;
        }
      }

      for (FileSet<CopyEntity> fileSetToRemove : partitionsToDelete) {
        List<WorkUnit> workUnitsRemoved = this.workUnitsMap.remove(fileSetToRemove);
        this.currentSize -= workUnitsRemoved.size();
      }
    }

    // TreeMap determines key equality using provided comparator. If multiple fileSets have same priority, we need
    // to concat their work units, otherwise only the last one will survive. Obviously, the comparator must be
    // transitive, but it need not be consistent with equals.
    if (!this.workUnitsMap.containsKey(fileSet)) {
      this.workUnitsMap.put(fileSet, workUnits);
    } else {
      this.workUnitsMap.get(fileSet).addAll(workUnits);
    }

    this.currentSize += workUnits.size();
    log.info(String.format("Added %d work units to bounded list. Total size: %d, soft limit: %d, hard limit: %d.",
        workUnits.size(), this.currentSize, this.maxSize, this.strictMaxSize));
    return true;
  }

  /**
   * @return Whether any calls to {@link #addFileSet} have returned false, i.e. some file set has been rejected due
   * to strict capacity issues.
   */
  public boolean hasRejectedFileSet() {
    return this.rejectedFileSet;
  }

  /**
   * @return Whether the list has reached its max size.
   */
  public synchronized boolean isFull() {
    return this.currentSize >= this.maxSize;
  }

  /**
   * Get the {@link List} of {@link WorkUnit}s in this container.
   */
  public List<WorkUnit> getWorkUnits() {
    ImmutableList.Builder<WorkUnit> allWorkUnits = ImmutableList.builder();
    for (List<WorkUnit> workUnits : this.workUnitsMap.values()) {
      allWorkUnits.addAll(workUnits);
    }
    return allWorkUnits.build();
  }

  /**
   * Get the raw map backing this object.
   */
  public Map<FileSet<CopyEntity>, List<WorkUnit>> getRawWorkUnitMap() {
    return this.workUnitsMap;
  }
}
