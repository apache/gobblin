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

package org.apache.gobblin.runtime.mapreduce;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.gobblin.util.binpacking.WorstFitDecreasingBinPacking;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.NodeBase;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.source.workunit.WorkUnit;


/**
 * An input format for Gobblin inputs (work units) which have been split into block-level work units;
 * assigns work units to input splits based on rack locality.
 * Uses the gobblin.inputsplit related properties in {@link GobblinWorkUnitsInputFormat}.
 * To use this input format, instead of the default {@link GobblinWorkUnitsInputFormat},
 * use the {@link MRJobLauncher#INPUT_FORMAT_CLASS_KEY} property
 */
@Slf4j
public class RackLocalGobblinWorkUnitsInputFormat extends GobblinWorkUnitsInputFormat {

  // mapping from a rack name to the set of relevant nodes in the rack
  Map<String, Set<String>> racksToNodes = Maps.newHashMap();
  // mapping from a node to the set of relevant block-level work units that it contains
  Map<String, Set<String>> nodesToWorkUnits = Maps.newHashMap();
  // mapping from a rack name to the list of relevant block-level work units it contains
  Map<String, Set<String>> racksToWorkUnits = Maps.newHashMap();

  @Override
  protected void clearWorkUnitPathsInfo() {
    racksToNodes.clear();
    nodesToWorkUnits.clear();
    racksToWorkUnits.clear();
    super.clearWorkUnitPathsInfo();
  }

  /**
   * Hook to populate the maps with info about the work unit, which is to be used later when creating the splits.
   * @param workUnitPath
   * @param fs
   * @throws IOException
   */
  @Override
  protected void addWorkUnitPathInfo(Path workUnitPath, FileSystem fs) throws IOException {
    WorkUnit workUnit = deserializeWorkUnitWrapper(workUnitPath, fs);
    workUnitPaths.add(workUnitPath.toString());

    long totalLength = 0L;
    Map<String, Long> lengthsForBlkLocations = Maps.newHashMap();
    if (workUnit.contains(GOBBLIN_SPLIT_FILE_PATH)) {
      Path filePath = new Path(workUnit.getProp(GOBBLIN_SPLIT_FILE_PATH));
      long offset = workUnit.getPropAsLong(GOBBLIN_SPLIT_FILE_LOW_POSITION, 0L);
      totalLength = (workUnit.contains(GOBBLIN_SPLIT_FILE_HIGH_POSITION) ?
          workUnit.getPropAsLong(GOBBLIN_SPLIT_FILE_HIGH_POSITION) : fs.getFileStatus(filePath).getLen()) - offset;
      workUnitLengths.put(workUnitPath.toString(), totalLength);

      BlockLocation[] blkLocations = fs.getFileBlockLocations(filePath, offset, totalLength);
      if (blkLocations != null && blkLocations.length > 0) {
        // just take/use first block location retrieved if more than 1 since they should be block-level work units
        String[] hosts = blkLocations[0].getHosts();
        String[] topos = blkLocations[0].getTopologyPaths();

        for (String host : hosts) {
          nodesToWorkUnits.computeIfAbsent(host, h -> Sets.newHashSet()).add(workUnitPath.toString());
        }

        parseToBlkLocationLengthsMap(blkLocations, offset, totalLength, lengthsForBlkLocations);
        workUnitBlkLocationsMap.put(workUnitPath.toString(), lengthsForBlkLocations);

        if (topos.length == 0) {
          topos = new String[hosts.length];
          for (int i = 0; i < topos.length; ++i) {
            topos[i] = (new NodeBase(hosts[i], NetworkTopology.DEFAULT_RACK)).toString();
          }
        }

        for (int i = 0; i < topos.length; ++i) {
          String rack = (new NodeBase(topos[i])).getNetworkLocation();
          racksToWorkUnits.computeIfAbsent(rack, r -> Sets.newHashSet()).add(workUnitPath.toString());

          if (!rack.equals(NetworkTopology.DEFAULT_RACK)) {
            racksToNodes.computeIfAbsent(rack, r -> Sets.newHashSet()).add(hosts[i]);
          }
        }
      }
    } else {
      totalLength = workUnit.getPropAsLong(WorstFitDecreasingBinPacking.TOTAL_MULTI_WORK_UNIT_WEIGHT,
          workUnit.getPropAsLong(WORK_UNIT_WEIGHT, 0L));
      workUnitLengths.put(workUnitPath.toString(), totalLength);
      workUnitBlkLocationsMap.put(workUnitPath.toString(), lengthsForBlkLocations);
    }
  }

  @Override
  protected void createSplits(List<InputSplit> splits, int numTasksPerMapper) {
    iterateAndCreateSplits(splits, numTasksPerMapper, nodesToWorkUnits);
    iterateAndCreateSplits(splits, numTasksPerMapper, racksToWorkUnits);
    super.createSplits(splits, numTasksPerMapper);
  }

  /**
   * Iterate through reverse map and create data-local splits for each key if possible. Generate max one split
   * per key iteration, so that splits can be better distributed amongst the constructs that the keys represent,
   * but we potentially touch keys multiple times from different iterations if we've iterated through all the
   * keys already and it is still possible to create a split for a key where a split has already been created.
   */
  private void iterateAndCreateSplits(List<InputSplit> splits, int numTasksPerMapper,
      Map<String, Set<String>> reverseWorkUnitsMap) {
    Set<String> completed = Sets.newHashSet();
    while (completed.size() < reverseWorkUnitsMap.size()) {
      for (Map.Entry<String, Set<String>> workUnitsEntry : reverseWorkUnitsMap.entrySet()) {
        String key = workUnitsEntry.getKey();
        if (completed.contains(key)) {
          continue;
        }

        Set<String> validWorkUnits = Sets.newHashSet();
        Set<String> workUnits = workUnitsEntry.getValue();
        Iterator<String> workUnitsIter = workUnits.iterator();
        while (workUnitsIter.hasNext()) {
          String workUnit = workUnitsIter.next();

          if (!workUnitPaths.contains(workUnit)) {
            workUnitsIter.remove();
            continue;
          }

          validWorkUnits.add(workUnit);
          if (validWorkUnits.size() == numTasksPerMapper) {
            // call get hosts to get a list of node locations for the given key - if the key is a rack,
            // use the rackstonodes map to get a list of nodes, if the key is a node itself, the method
            // will return a singleton list
            splits.add(createSplit(validWorkUnits, getHosts(key)));

            workUnits.removeAll(validWorkUnits);
            workUnitPaths.removeAll(validWorkUnits);

            validWorkUnits.clear();

            break;
          }
        }

        if (workUnits.size() == validWorkUnits.size()) {
          completed.add(key);
        }
      }
    }
  }

  private GobblinSplit createSplit(Set<String> workUnitsToAdd, String[] locations) {
    List<String> splitPaths = Lists.newArrayListWithExpectedSize(workUnitsToAdd.size());
    long splitLength = 0L;
    for (String workUnit : workUnitsToAdd) {
      splitPaths.add(workUnit);
      splitLength += workUnitLengths.get(workUnit);
    }
    return new GobblinSplit(splitPaths, splitLength, locations);
  }

  private String[] getHosts(String key) {
    Set<String> hosts = Sets.newHashSet();
    if (racksToNodes.containsKey(key)) {
      hosts.addAll(racksToNodes.get(key));
    } else {
      hosts.add(key);
    }
    return hosts.toArray(new String[hosts.size()]);
  }

}
