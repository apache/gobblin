/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package gobblin.cluster;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.helix.ZNRecord;
import org.apache.helix.controller.stages.ClusterDataCache;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.controller.strategy.AutoRebalanceStrategy;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Partition;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobContext;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskPartitionState;
import org.apache.helix.task.TaskRebalancer;
import org.apache.helix.task.WorkflowConfig;
import org.apache.helix.task.WorkflowContext;

import com.google.common.base.Function;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import lombok.extern.slf4j.Slf4j;


/**
 * Implemented as a workaround until we fix the underlying Helix {@link org.apache.helix.task.GenericTaskRebalancer}
 * to support rebalancing running tasks. This class might go away if we fix this in Helix directly.
 */
@Slf4j
public class GobblinTaskRebalancer extends TaskRebalancer {

  public Set<Integer> getAllTaskPartitions(JobConfig jobCfg, JobContext jobCtx, WorkflowConfig workflowCfg, WorkflowContext workflowCtx, ClusterDataCache cache) {
    Map taskMap = jobCfg.getTaskConfigMap();
    Map taskIdMap = jobCtx.getTaskIdPartitionMap();
    Iterator i$ = taskMap.values().iterator();

    while(i$.hasNext()) {
      TaskConfig taskCfg = (TaskConfig)i$.next();
      String taskId = taskCfg.getId();
      int nextPartition = jobCtx.getPartitionSet().size();
      if(!taskIdMap.containsKey(taskId)) {
        jobCtx.setTaskIdForPartition(nextPartition, taskId);
      }
    }

    return jobCtx.getPartitionSet();
  }


  public Map<String, SortedSet<Integer>> getTaskAssignment(CurrentStateOutput currStateOutput, ResourceAssignment prevAssignment, Collection<String> instances, JobConfig jobCfg, JobContext jobContext, WorkflowConfig workflowCfg, WorkflowContext workflowCtx, Set<Integer> partitionSet, ClusterDataCache cache) {
    LinkedHashMap states = new LinkedHashMap();
    states.put("ONLINE", Integer.valueOf(1));
    log.info("Asked to get task assignment");
    HashSet honoredStates = Sets.newHashSet(new TaskPartitionState[]{TaskPartitionState.INIT, TaskPartitionState.STOPPED});
    HashSet filteredPartitionSet = Sets.newHashSet();
    Iterator partitionNums = partitionSet.iterator();

    while(partitionNums.hasNext()) {
      Integer resourceId = (Integer)partitionNums.next();
      TaskPartitionState partitions = jobContext == null?null:jobContext.getPartitionState(resourceId.intValue());
      if(partitions == null || honoredStates.contains(partitions)) {
        filteredPartitionSet.add(resourceId);
      }
    }

    ArrayList partitionNums1 = Lists.newArrayList(partitionSet);
    Collections.sort(partitionNums1);
    final String resourceId1 = prevAssignment.getResourceName();
    ArrayList partitions1 = new ArrayList(Lists.transform(partitionNums1, new Function<Integer, String>() {

      public String apply(Integer partitionNum) {
        return resourceId1 + "_" + partitionNum;
      }
    }));
    HashMap currentMapping = Maps.newHashMap();
    Iterator strategy = currStateOutput.getCurrentStateMappedPartitions(resourceId1).iterator();

    while(strategy.hasNext()) {
      Partition allNodes = (Partition)strategy.next();
      if(filteredPartitionSet.contains(Integer.valueOf(pId(allNodes.getPartitionName())))) {
        HashMap record = Maps.newHashMap();
        if(prevAssignment != null) {
          record.putAll(prevAssignment.getReplicaMap(allNodes));
        }

        record.putAll(currStateOutput.getCurrentStateMap(resourceId1, allNodes));
        record.putAll(currStateOutput.getPendingStateMap(resourceId1, allNodes));
        currentMapping.put(allNodes.getPartitionName(), record);
      }
    }

    AutoRebalanceStrategy
        strategy1 = new AutoRebalanceStrategy(resourceId1, partitions1, states, 2147483647, new AutoRebalanceStrategy.DefaultPlacementScheme());
    ArrayList allNodes1 = Lists.newArrayList(this.getEligibleInstances(jobCfg, currStateOutput, instances, cache));
    Collections.sort(allNodes1);
    ZNRecord record1 = strategy1.computePartitionAssignment(allNodes1, currentMapping, allNodes1);
    Map preferenceLists = record1.getListFields();
    HashMap taskAssignment = Maps.newHashMap();
    Iterator i$ = preferenceLists.entrySet().iterator();

    while(i$.hasNext()) {
      Map.Entry e = (Map.Entry)i$.next();
      String partitionName = (String)e.getKey();
      partitionName = String.valueOf(pId(partitionName));
      List preferenceList = (List)e.getValue();

      String participantName;
      for(Iterator i$1 = preferenceList.iterator(); i$1.hasNext(); ((SortedSet)taskAssignment.get(participantName)).add(Integer.valueOf(partitionName))) {
        participantName = (String)i$1.next();
        if(!taskAssignment.containsKey(participantName)) {
          taskAssignment.put(participantName, new TreeSet());
        }
      }
    }

    Map taskAssignment1 = this._retryPolicy.reassign(jobCfg, jobContext, allNodes1, taskAssignment);
    log.info("Returning task assignment : {}", taskAssignment1);
    return taskAssignment1;
  }

  private RetryPolicy _retryPolicy = new DefaultRetryReassigner();


  private Set<String> getEligibleInstances(JobConfig jobCfg, CurrentStateOutput currStateOutput, Iterable<String> instances, ClusterDataCache cache) {
    HashSet allInstances = Sets.newHashSet(instances);
    String targetResource = jobCfg.getTargetResource();
    if(targetResource == null) {
      return allInstances;
    } else {
      IdealState idealState = cache.getIdealState(targetResource);
      if(idealState == null) {
        return Collections.emptySet();
      } else {
        Set partitions = idealState.getPartitionSet();
        List targetPartitions = jobCfg.getTargetPartitions();
        if(targetPartitions != null && !targetPartitions.isEmpty()) {
          partitions.retainAll(targetPartitions);
        }

        HashSet eligibleInstances = Sets.newHashSet();
        Set targetStates = jobCfg.getTargetPartitionStates();
        Iterator i$ = partitions.iterator();

        label50:
        while(i$.hasNext()) {
          String partition = (String)i$.next();
          Map stateMap = currStateOutput.getCurrentStateMap(targetResource, new Partition(partition));
          Map pendingStateMap = currStateOutput.getPendingStateMap(targetResource, new Partition(partition));
          Iterator i$1 = stateMap.entrySet().iterator();

          while(true) {
            String instanceName;
            String state;
            String pending;
            do {
              do {
                if(!i$1.hasNext()) {
                  continue label50;
                }

                Map.Entry e = (Map.Entry)i$1.next();
                instanceName = (String)e.getKey();
                state = (String)e.getValue();
                pending = (String)pendingStateMap.get(instanceName);
              } while(pending != null);
            } while(targetStates != null && !targetStates.isEmpty() && !targetStates.contains(state));

            eligibleInstances.add(instanceName);
          }
        }

        allInstances.retainAll(eligibleInstances);
        return allInstances;
      }
    }
  }

  private static class DefaultRetryReassigner implements RetryPolicy {
    private DefaultRetryReassigner() {
    }

    public Map<String, SortedSet<Integer>> reassign(JobConfig jobCfg, JobContext jobCtx, Collection<String> instances, Map<String, SortedSet<Integer>> origAssignment) {
      HashBiMap instanceMap = HashBiMap.create(instances.size());
      int instanceIndex = 0;
      Iterator newAssignment = instances.iterator();

      while(newAssignment.hasNext()) {
        String i$ = (String)newAssignment.next();
        instanceMap.put(i$, Integer.valueOf(instanceIndex++));
      }

      HashMap var18 = Maps.newHashMap();
      Iterator var19 = origAssignment.entrySet().iterator();

      while(true) {
        while(var19.hasNext()) {
          Map.Entry e = (Map.Entry)var19.next();
          String instance = (String)e.getKey();
          SortedSet partitions = (SortedSet)e.getValue();
          Integer instanceId = (Integer)instanceMap.get(instance);
          int p;
          String newInstance;
          if(instanceId != null) {
            for(Iterator i$1 = partitions.iterator(); i$1.hasNext(); ((SortedSet)var18.get(newInstance)).add(Integer.valueOf(p))) {
              p = ((Integer)i$1.next()).intValue();
              int shiftValue = this.getNumInstancesToShift(jobCfg, jobCtx, instances, p);
              int newInstanceId = (instanceId.intValue() + shiftValue) % instances.size();
              newInstance = (String)instanceMap.inverse().get(Integer.valueOf(newInstanceId));
              if(newInstance == null) {
                newInstance = instance;
              }

              if(!var18.containsKey(newInstance)) {
                var18.put(newInstance, new TreeSet());
              }
            }
          } else {
            var18.put(instance, partitions);
          }
        }

        return var18;
      }
    }

    private int getNumInstancesToShift(JobConfig jobCfg, JobContext jobCtx, Collection<String> instances, int p) {
      int numAttempts = jobCtx.getPartitionNumAttempts(p);
      int maxNumAttempts = jobCfg.getMaxAttemptsPerTask();
      int numInstances = Math.min(instances.size(), jobCfg.getMaxForcedReassignmentsPerTask() + 1);
      return numAttempts / (maxNumAttempts / numInstances);
    }
  }

  public interface RetryPolicy {
    Map<String, SortedSet<Integer>> reassign(JobConfig var1, JobContext var2, Collection<String> var3, Map<String, SortedSet<Integer>> var4);
  }

}