/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.data.management.copy;

import lombok.Getter;

import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import gobblin.data.management.partition.FileSet;
import gobblin.source.workunit.WorkUnit;


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
class ConcurrentBoundedWorkUnitList {

  private final TreeMap<FileSet<CopyableFile>, List<WorkUnit>> workUnitsMap;
  @Getter
  private final Comparator<FileSet<CopyableFile>> comparator;
  private final int maxSize;
  private int currentSize;
  /** Set to true the first time a file set is rejected (i.e. doesn't fit in the container) */
  private boolean rejectedFileSet;

  private class AugmentedComparator implements Comparator<FileSet<CopyableFile>> {
    private final Comparator<FileSet<CopyableFile>> userProvidedComparator;

    public AugmentedComparator(Comparator<FileSet<CopyableFile>> userProvidedComparator) {
      this.userProvidedComparator = userProvidedComparator;
    }

    @Override public int compare(FileSet<CopyableFile> p1, FileSet<CopyableFile> p2) {
      int userProvidedCompare = this.userProvidedComparator.compare(p1, p2);
      if (userProvidedCompare == 0) {
        int datasetCompare = p1.getDataset().datasetRoot().compareTo(p2.getDataset().datasetRoot());
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
   */
  public ConcurrentBoundedWorkUnitList(int maxSize, final Comparator<FileSet<CopyableFile>> comparator) {
    this.currentSize = 0;
    this.maxSize = maxSize;
    this.comparator = comparator;
    this.workUnitsMap = new TreeMap<>(new AugmentedComparator(comparator));
    this.rejectedFileSet = false;
  }

  /**
   * Add a file set to the container.
   * @param fileSet File set, expressed as a {@link gobblin.data.management.partition.FileSet} of {@link CopyableFile}s.
   * @param workUnits List of {@link WorkUnit}s corresponding to this file set.
   * @return true if the file set was added to the container, false otherwise (i.e. has reached max size).
   */
  public boolean addFileSet(FileSet<CopyableFile> fileSet, List<WorkUnit> workUnits) {
    boolean addedWorkunits = addFileSetImpl(fileSet, workUnits);
    if (!addedWorkunits) {
      this.rejectedFileSet = true;
    }
    return addedWorkunits;
  }

  private synchronized boolean addFileSetImpl(FileSet<CopyableFile> fileSet, List<WorkUnit> workUnits) {
    if (this.currentSize + workUnits.size() > this.maxSize) {
      if (this.comparator.compare(this.workUnitsMap.lastKey(), fileSet) <= 0) {
        return false;
      }
      int tmpSize = this.currentSize;
      Set<FileSet<CopyableFile>> partitionsToDelete = Sets.newHashSet();

      for (FileSet<CopyableFile> existingFileSet : this.workUnitsMap.descendingKeySet()) {
        if (this.comparator.compare(existingFileSet, fileSet) <= 0) {
          return false;
        }
        tmpSize -= this.workUnitsMap.get(existingFileSet).size();
        partitionsToDelete.add(existingFileSet);
        if (tmpSize + workUnits.size() <= this.maxSize) {
          break;
        }
      }

      for (FileSet<CopyableFile> fileSetToRemove : partitionsToDelete) {
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
    return true;
  }

  /**
   * @return Whether any calls to {@link #addFileSet} have returned false, i.e. some file set has been rejected due
   * to capacity issues.
   */
  public boolean hasRejectedFileSet() {
    return rejectedFileSet;
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
}
