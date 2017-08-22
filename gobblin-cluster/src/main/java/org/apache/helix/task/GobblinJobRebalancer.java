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

package org.apache.helix.task;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.helix.AccessOption;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.ZNRecord;
import org.apache.helix.controller.stages.ClusterDataCache;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceAssignment;
import org.apache.log4j.Logger;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

/**
 * Custom rebalancer implementation for the {@code Job} in task model.
 */
public class GobblinJobRebalancer extends TaskRebalancer {
  private static final Logger LOG = Logger.getLogger(GobblinJobRebalancer.class);
  private static TaskAssignmentCalculator fixTaskAssignmentCal =
      new FixedTargetTaskAssignmentCalculator();
  private static TaskAssignmentCalculator genericTaskAssignmentCal =
      new GenericTaskAssignmentCalculator();

  private static final String PREV_RA_NODE = "PreviousResourceAssignment";

  @Override
  public ResourceAssignment computeBestPossiblePartitionState(ClusterDataCache clusterData,
      IdealState taskIs, Resource resource, CurrentStateOutput currStateOutput) {
    final String jobName = resource.getResourceName();
    LOG.debug("Computer Best Partition for job: " + jobName);

    // Fetch job configuration
    JobConfig jobCfg = TaskUtil.getJobCfg(_manager, jobName);
    if (jobCfg == null) {
      LOG.error("Job configuration is NULL for " + jobName);
      return buildEmptyAssignment(jobName, currStateOutput);
    }
    String workflowResource = jobCfg.getWorkflow();

    // Fetch workflow configuration and context
    WorkflowConfig workflowCfg = TaskUtil.getWorkflowCfg(_manager, workflowResource);
    if (workflowCfg == null) {
      LOG.error("Workflow configuration is NULL for " + jobName);
      return buildEmptyAssignment(jobName, currStateOutput);
    }

    WorkflowContext workflowCtx = TaskUtil.getWorkflowContext(_manager, workflowResource);
    if (workflowCtx == null) {
      LOG.error("Workflow context is NULL for " + jobName);
      return buildEmptyAssignment(jobName, currStateOutput);
    }

    TargetState targetState = workflowCfg.getTargetState();
    if (targetState != TargetState.START && targetState != TargetState.STOP) {
      LOG.info("Target state is " + targetState.name() + " for workflow " + workflowResource
          + ".Stop scheduling job " + jobName);
      return buildEmptyAssignment(jobName, currStateOutput);
    }

    // Stop current run of the job if workflow or job is already in final state (failed or completed)
    TaskState workflowState = workflowCtx.getWorkflowState();
    TaskState jobState = workflowCtx.getJobState(jobName);
    // The job is already in a final state (completed/failed).
    if (workflowState == TaskState.FAILED || workflowState == TaskState.COMPLETED ||
        jobState == TaskState.FAILED || jobState == TaskState.COMPLETED) {
      LOG.info(String.format(
          "Workflow %s or job %s is already failed or completed, workflow state (%s), job state (%s), clean up job IS.",
          workflowResource, jobName, workflowState, jobState));
      cleanupIdealStateExtView(_manager.getHelixDataAccessor(), jobName);
      _scheduledRebalancer.removeScheduledRebalance(jobName);
      return buildEmptyAssignment(jobName, currStateOutput);
    }

    if (!isWorkflowReadyForSchedule(workflowCfg)) {
      LOG.info("Job is not ready to be run since workflow is not ready " + jobName);
      return buildEmptyAssignment(jobName, currStateOutput);
    }

    if (!isJobStarted(jobName, workflowCtx) && !isJobReadyToSchedule(jobName, workflowCfg,
        workflowCtx)) {
      LOG.info("Job is not ready to run " + jobName);
      return buildEmptyAssignment(jobName, currStateOutput);
    }

    // Fetch any existing context information from the property store.
    JobContext jobCtx = TaskUtil.getJobContext(_manager, jobName);
    if (jobCtx == null) {
      jobCtx = new JobContext(new ZNRecord("TaskContext"));
      jobCtx.setStartTime(System.currentTimeMillis());
    }

    // Grab the old assignment, or an empty one if it doesn't exist
    ResourceAssignment prevAssignment = getPrevResourceAssignment(jobName);
    if (prevAssignment == null) {
      prevAssignment = new ResourceAssignment(jobName);
    }

    // Will contain the list of partitions that must be explicitly dropped from the ideal state that
    // is stored in zk.
    // Fetch the previous resource assignment from the property store. This is required because of
    // HELIX-230.
    Set<String> liveInstances = jobCfg.getInstanceGroupTag() == null
        ? clusterData.getAllEnabledLiveInstances()
        : clusterData.getAllEnabledLiveInstancesWithTag(jobCfg.getInstanceGroupTag());

    if (liveInstances.isEmpty()) {
      LOG.error("No available instance found for job!");
    }

    Set<Integer> partitionsToDrop = new TreeSet<Integer>();
    ResourceAssignment newAssignment =
        computeResourceMapping(jobName, workflowCfg, jobCfg, prevAssignment, liveInstances,
            currStateOutput, workflowCtx, jobCtx, partitionsToDrop, clusterData);

    if (!partitionsToDrop.isEmpty()) {
      for (Integer pId : partitionsToDrop) {
        taskIs.getRecord().getMapFields().remove(pName(jobName, pId));
      }
      HelixDataAccessor accessor = _manager.getHelixDataAccessor();
      PropertyKey propertyKey = accessor.keyBuilder().idealStates(jobName);
      accessor.setProperty(propertyKey, taskIs);
    }

    // Update rebalancer context, previous ideal state.
    TaskUtil.setJobContext(_manager, jobName, jobCtx);
    TaskUtil.setWorkflowContext(_manager, workflowResource, workflowCtx);
    setPrevResourceAssignment(jobName, newAssignment);

    LOG.debug("Job " + jobName + " new assignment " + Arrays
        .toString(newAssignment.getMappedPartitions().toArray()));
    return newAssignment;
  }

  private Set<String> getInstancesAssignedToOtherJobs(String currentJobName,
      WorkflowConfig workflowCfg) {
    Set<String> ret = new HashSet<String>();
    for (String jobName : workflowCfg.getJobDag().getAllNodes()) {
      if (jobName.equals(currentJobName)) {
        continue;
      }
      JobContext jobContext = TaskUtil.getJobContext(_manager, jobName);
      if (jobContext == null) {
        continue;
      }
      for (int partition : jobContext.getPartitionSet()) {
        TaskPartitionState partitionState = jobContext.getPartitionState(partition);
        if (partitionState == TaskPartitionState.INIT ||
            partitionState == TaskPartitionState.RUNNING) {
          ret.add(jobContext.getAssignedParticipant(partition));
        }
      }
    }

    return ret;
  }

  private ResourceAssignment computeResourceMapping(String jobResource,
      WorkflowConfig workflowConfig, JobConfig jobCfg, ResourceAssignment prevAssignment,
      Collection<String> liveInstances, CurrentStateOutput currStateOutput,
      WorkflowContext workflowCtx, JobContext jobCtx, Set<Integer> partitionsToDropFromIs,
      ClusterDataCache cache) {
    TargetState jobTgtState = workflowConfig.getTargetState();
    // Update running status in workflow context
    if (jobTgtState == TargetState.STOP) {
      workflowCtx.setJobState(jobResource, TaskState.STOPPED);
      // Workflow has been stopped if all in progress jobs are stopped
      if (isWorkflowStopped(workflowCtx, workflowConfig)) {
        workflowCtx.setWorkflowState(TaskState.STOPPED);
      }
    } else {
      workflowCtx.setJobState(jobResource, TaskState.IN_PROGRESS);
      // Workflow is in progress if any task is in progress
      workflowCtx.setWorkflowState(TaskState.IN_PROGRESS);
    }

    // Used to keep track of tasks that have already been assigned to instances.
    Set<Integer> assignedPartitions = new HashSet<Integer>();

    // Used to keep track of tasks that have failed, but whose failure is acceptable
    Set<Integer> skippedPartitions = new HashSet<Integer>();

    // Keeps a mapping of (partition) -> (instance, state)
    Map<Integer, PartitionAssignment> paMap = new TreeMap<Integer, PartitionAssignment>();

    // Keeps a mapping of (partition) -> (instance, state) of partitions have have been relocated
    Map<Integer, PartitionAssignment> relocatedPaMap = new TreeMap<Integer, PartitionAssignment>();

    Set<String> excludedInstances = getInstancesAssignedToOtherJobs(jobResource, workflowConfig);

    // Process all the current assignments of tasks.
    TaskAssignmentCalculator taskAssignmentCal = getAssignmentCalulator(jobCfg);
    Set<Integer> allPartitions = taskAssignmentCal
        .getAllTaskPartitions(jobCfg, jobCtx, workflowConfig, workflowCtx, cache.getIdealStates());

    if (allPartitions == null || allPartitions.isEmpty()) {
      // Empty target partitions, mark the job as FAILED.
      String failureMsg = "Empty task partition mapping for job " + jobResource + ", marked the job as FAILED!";
      LOG.info(failureMsg);
      jobCtx.setInfo(failureMsg);
      markJobFailed(jobResource, jobCtx, workflowConfig, workflowCtx);
      markAllPartitionsError(jobCtx, TaskPartitionState.ERROR, false);
      _clusterStatusMonitor.updateJobCounters(jobCfg, TaskState.FAILED);
      return new ResourceAssignment(jobResource);
    }

    Map<String, SortedSet<Integer>> taskAssignments =
        getTaskPartitionAssignments(liveInstances, prevAssignment, allPartitions);
    long currentTime = System.currentTimeMillis();

    LOG.debug("All partitions: " + allPartitions + " taskAssignment: " + taskAssignments
        + " excludedInstances: " + excludedInstances);

    for (Map.Entry<String, SortedSet<Integer>> entryInstance : taskAssignments.entrySet()) {
      String instance = entryInstance.getKey();
      if (excludedInstances.contains(instance)) {
        continue;
      }

      Set<Integer> pSet = entryInstance.getValue();
      // Used to keep track of partitions that are in one of the final states: COMPLETED, TIMED_OUT,
      // TASK_ERROR, ERROR.
      Set<Integer> donePartitions = new TreeSet<Integer>();
      for (int pId : pSet) {
        final String pName = pName(jobResource, pId);

        // Check for pending state transitions on this (partition, instance).
        Message pendingMessage =
            currStateOutput.getPendingState(jobResource, new Partition(pName), instance);
        if (pendingMessage != null) {
          // There is a pending state transition for this (partition, instance). Just copy forward
          // the state assignment from the previous ideal state.
          Map<String, String> stateMap = prevAssignment.getReplicaMap(new Partition(pName));
          if (stateMap != null) {
            String prevState = stateMap.get(instance);
            paMap.put(pId, new PartitionAssignment(instance, prevState));
            assignedPartitions.add(pId);
            if (LOG.isDebugEnabled()) {
              LOG.debug(String.format(
                  "Task partition %s has a pending state transition on instance %s. Using the previous ideal state which was %s.",
                  pName, instance, prevState));
            }
          }

          continue;
        }

        TaskPartitionState currState =
            TaskPartitionState.valueOf(currStateOutput.getCurrentState(jobResource, new Partition(
                pName), instance));
        jobCtx.setPartitionState(pId, currState);

        String taskMsg = currStateOutput.getInfo(jobResource, new Partition(
            pName), instance);
        if (taskMsg != null) {
          jobCtx.setPartitionInfo(pId, taskMsg);
        }

        // Process any requested state transitions.
        String requestedStateStr =
            currStateOutput.getRequestedState(jobResource, new Partition(pName), instance);
        if (requestedStateStr != null && !requestedStateStr.isEmpty()) {
          TaskPartitionState requestedState = TaskPartitionState.valueOf(requestedStateStr);
          if (requestedState.equals(currState)) {
            LOG.warn(String.format(
                "Requested state %s is the same as the current state for instance %s.",
                requestedState, instance));
          }

          paMap.put(pId, new PartitionAssignment(instance, requestedState.name()));
          assignedPartitions.add(pId);
          LOG.debug(String.format(
              "Instance %s requested a state transition to %s for partition %s.", instance,
              requestedState, pName));
          continue;
        }

        switch (currState) {
          case RUNNING:
          case STOPPED: {
            TaskPartitionState nextState;
            if (jobTgtState == TargetState.START) {
              nextState = TaskPartitionState.RUNNING;
            } else {
              nextState = TaskPartitionState.STOPPED;
            }

            paMap.put(pId, new PartitionAssignment(instance, nextState.name()));
            assignedPartitions.add(pId);
            LOG.debug(String.format("Setting task partition %s state to %s on instance %s.", pName,
                nextState, instance));
          }
          break;
          case COMPLETED: {
            // The task has completed on this partition. Mark as such in the context object.
            donePartitions.add(pId);
            LOG.debug(String
                .format(
                    "Task partition %s has completed with state %s. Marking as such in rebalancer context.",
                    pName, currState));
            partitionsToDropFromIs.add(pId);
            markPartitionCompleted(jobCtx, pId);
          }
          break;
          case TIMED_OUT:
          case TASK_ERROR:
          case TASK_ABORTED:
          case ERROR: {
            donePartitions.add(pId); // The task may be rescheduled on a different instance.
            LOG.debug(String.format(
                "Task partition %s has error state %s with msg %s. Marking as such in rebalancer context.", pName,
                currState, taskMsg));
            markPartitionError(jobCtx, pId, currState, true);
            // The error policy is to fail the task as soon a single partition fails for a specified
            // maximum number of attempts or task is in ABORTED state.
            if (jobCtx.getPartitionNumAttempts(pId) >= jobCfg.getMaxAttemptsPerTask() ||
                currState.equals(TaskPartitionState.TASK_ABORTED)) {
              // If we have some leeway for how many tasks we can fail, then we don't have
              // to fail the job immediately
              if (skippedPartitions.size() >= jobCfg.getFailureThreshold()) {
                markJobFailed(jobResource, jobCtx, workflowConfig, workflowCtx);
                _clusterStatusMonitor.updateJobCounters(jobCfg, TaskState.FAILED);
                markAllPartitionsError(jobCtx, currState, false);
                addAllPartitions(allPartitions, partitionsToDropFromIs);

                // remove IdealState of this job
                cleanupIdealStateExtView(_manager.getHelixDataAccessor(), jobResource);
                return buildEmptyAssignment(jobResource, currStateOutput);
              } else {
                skippedPartitions.add(pId);
                partitionsToDropFromIs.add(pId);
              }

              LOG.debug("skippedPartitions:" + skippedPartitions);
            } else {
              // Mark the task to be started at some later time (if enabled)
              markPartitionDelayed(jobCfg, jobCtx, pId);
            }
          }
          break;
          case INIT:
          case DROPPED: {
            // currState in [INIT, DROPPED]. Do nothing, the partition is eligible to be reassigned.
            donePartitions.add(pId);
            LOG.debug(String.format(
                "Task partition %s has state %s. It will be dropped from the current ideal state.",
                pName, currState));
          }
          break;
          default:
            throw new AssertionError("Unknown enum symbol: " + currState);
        }
      }

      // Remove the set of task partitions that are completed or in one of the error states.
      pSet.removeAll(donePartitions);
    }

    // For delayed tasks, trigger a rebalance event for the closest upcoming ready time
    scheduleForNextTask(jobResource, jobCtx, currentTime);

    if (isJobComplete(jobCtx, allPartitions, skippedPartitions, jobCfg)) {
      markJobComplete(jobResource, jobCtx, workflowConfig, workflowCtx);
      _clusterStatusMonitor.updateJobCounters(jobCfg, TaskState.COMPLETED);
      // remove IdealState of this job
      cleanupIdealStateExtView(_manager.getHelixDataAccessor(), jobResource);
    }

    // Make additional task assignments if needed.
    if (jobTgtState == TargetState.START) {
      // Contains the set of task partitions that must be excluded from consideration when making
      // any new assignments.
      // This includes all completed, failed, delayed, and already assigned partitions.
      //Set<Integer> excludeSet = Sets.newTreeSet(assignedPartitions);
      //HACK: Modify excludeSet to be empty
      Set<Integer> excludeSet = Sets.newTreeSet();
      addCompletedTasks(excludeSet, jobCtx, allPartitions);
      addGiveupPartitions(excludeSet, jobCtx, allPartitions, jobCfg);
      excludeSet.addAll(skippedPartitions);
      excludeSet.addAll(getNonReadyPartitions(jobCtx, currentTime));
      // Get instance->[partition, ...] mappings for the target resource.
      Map<String, SortedSet<Integer>> tgtPartitionAssignments = taskAssignmentCal
          .getTaskAssignment(currStateOutput, prevAssignment, liveInstances, jobCfg, jobCtx,
              workflowConfig, workflowCtx, allPartitions, cache.getIdealStates());
      for (Map.Entry<String, SortedSet<Integer>> entry : taskAssignments.entrySet()) {
        String instance = entry.getKey();
        if (!tgtPartitionAssignments.containsKey(instance) || excludedInstances
            .contains(instance)) {
          continue;
        }

        // Contains the set of task partitions currently assigned to the instance.
        Set<Integer> pSet = entry.getValue();
        int numToAssign = jobCfg.getNumConcurrentTasksPerInstance() - pSet.size();
        if (numToAssign > 0) {
          List<Integer> nextPartitions =
              getNextPartitions(tgtPartitionAssignments.get(instance), excludeSet, numToAssign);
          for (Integer pId : nextPartitions) {
            // if partition is not currently assigned to instance then it may have been moved
            if (!pSet.contains(pId)) {
              // look at current assignment to see if task is running on another instance
              for (Map.Entry<String, SortedSet<Integer>> currentEntry : taskAssignments.entrySet()) {
                String currentInstance = currentEntry.getKey();
                Set<Integer> currentpSet = currentEntry.getValue();

                // task is being moved, so transition to STOPPED state
                if (!instance.equals(currentInstance) && currentpSet.contains(pId)) {
                  relocatedPaMap.put(pId, new PartitionAssignment(currentInstance, TaskPartitionState.STOPPED.name()));
                  break;
                }
              }
            }

            String pName = pName(jobResource, pId);
            paMap.put(pId, new PartitionAssignment(instance, TaskPartitionState.RUNNING.name()));
            excludeSet.add(pId);
            jobCtx.setAssignedParticipant(pId, instance);
            jobCtx.setPartitionState(pId, TaskPartitionState.INIT);
            jobCtx.setPartitionStartTime(pId, System.currentTimeMillis());
            LOG.debug(String.format("Setting task partition %s state to %s on instance %s.", pName,
                TaskPartitionState.RUNNING, instance));
          }
        }
      }
    }

    // Construct a ResourceAssignment object from the map of partition assignments.
    ResourceAssignment ra = new ResourceAssignment(jobResource);
    for (Map.Entry<Integer, PartitionAssignment> e : paMap.entrySet()) {
      PartitionAssignment pa = e.getValue();

      if (relocatedPaMap.containsKey(e.getKey())) {
        PartitionAssignment currentPa = relocatedPaMap.get(e.getKey());

        ra.addReplicaMap(new Partition(pName(jobResource, e.getKey())),
            ImmutableMap.of(pa._instance, pa._state, currentPa._instance, currentPa._state));
      } else {
        ra.addReplicaMap(new Partition(pName(jobResource, e.getKey())), ImmutableMap.of(pa._instance, pa._state));
      }
    }

    return ra;
  }

  private void markJobComplete(String jobName, JobContext jobContext,
      WorkflowConfig workflowConfig, WorkflowContext workflowContext) {
    long currentTime = System.currentTimeMillis();
    workflowContext.setJobState(jobName, TaskState.COMPLETED);
    jobContext.setFinishTime(currentTime);
    if (isWorkflowFinished(workflowContext, workflowConfig)) {
      workflowContext.setFinishTime(currentTime);
    }
  }

  private void scheduleForNextTask(String job, JobContext jobCtx, long now) {
    // Clear current entries if they exist and are expired
    long currentTime = now;
    long scheduledTime = _scheduledRebalancer.getRebalanceTime(job);
    if (scheduledTime > 0 && currentTime > scheduledTime) {
      _scheduledRebalancer.removeScheduledRebalance(job);
    }

    // Figure out the earliest schedulable time in the future of a non-complete job
    boolean shouldSchedule = false;
    long earliestTime = Long.MAX_VALUE;
    for (int p : jobCtx.getPartitionSet()) {
      long retryTime = jobCtx.getNextRetryTime(p);
      TaskPartitionState state = jobCtx.getPartitionState(p);
      state = (state != null) ? state : TaskPartitionState.INIT;
      Set<TaskPartitionState> errorStates =
          Sets.newHashSet(TaskPartitionState.ERROR, TaskPartitionState.TASK_ERROR,
              TaskPartitionState.TIMED_OUT);
      if (errorStates.contains(state) && retryTime > currentTime && retryTime < earliestTime) {
        earliestTime = retryTime;
        shouldSchedule = true;
      }
    }

    // If any was found, then schedule it
    if (shouldSchedule) {
      _scheduledRebalancer.scheduleRebalance(_manager, job, earliestTime);
    }
  }

  /**
   * Get the last task assignment for a given job
   *
   * @param resourceName the name of the job
   * @return {@link ResourceAssignment} instance, or null if no assignment is available
   */
  private ResourceAssignment getPrevResourceAssignment(String resourceName) {
    ZNRecord r = _manager.getHelixPropertyStore()
        .get(Joiner.on("/").join(TaskConstants.REBALANCER_CONTEXT_ROOT, resourceName, PREV_RA_NODE),
            null, AccessOption.PERSISTENT);
    return r != null ? new ResourceAssignment(r) : null;
  }

  /**
   * Set the last task assignment for a given job
   *
   * @param resourceName the name of the job
   * @param ra           {@link ResourceAssignment} containing the task assignment
   */
  private void setPrevResourceAssignment(String resourceName,
      ResourceAssignment ra) {
    _manager.getHelixPropertyStore()
        .set(Joiner.on("/").join(TaskConstants.REBALANCER_CONTEXT_ROOT, resourceName, PREV_RA_NODE),
            ra.getRecord(), AccessOption.PERSISTENT);
  }

  /**
   * Checks if the job has completed.
   * @param ctx The rebalancer context.
   * @param allPartitions The set of partitions to check.
   * @param skippedPartitions partitions that failed, but whose failure is acceptable
   * @return true if all task partitions have been marked with status
   *         {@link TaskPartitionState#COMPLETED} in the rebalancer
   *         context, false otherwise.
   */
  private static boolean isJobComplete(JobContext ctx, Set<Integer> allPartitions,
      Set<Integer> skippedPartitions, JobConfig cfg) {
    for (Integer pId : allPartitions) {
      TaskPartitionState state = ctx.getPartitionState(pId);
      if (!skippedPartitions.contains(pId) && state != TaskPartitionState.COMPLETED
          && !isTaskGivenup(ctx, cfg, pId)) {
        return false;
      }
    }
    return true;
  }


  private static void addAllPartitions(Set<Integer> toAdd, Set<Integer> destination) {
    for (Integer pId : toAdd) {
      destination.add(pId);
    }
  }

  private static void addCompletedTasks(Set<Integer> set, JobContext ctx,
      Iterable<Integer> pIds) {
    for (Integer pId : pIds) {
      TaskPartitionState state = ctx.getPartitionState(pId);
      if (state == TaskPartitionState.COMPLETED) {
        set.add(pId);
      }
    }
  }

  private static boolean isTaskGivenup(JobContext ctx, JobConfig cfg, int pId) {
    TaskPartitionState state = ctx.getPartitionState(pId);
    if ((state != null) && (state.equals(TaskPartitionState.TASK_ABORTED) || state
        .equals(TaskPartitionState.ERROR))) {
      return true;
    }
    return ctx.getPartitionNumAttempts(pId) >= cfg.getMaxAttemptsPerTask();
  }

  // add all partitions that have been tried maxNumberAttempts
  private static void addGiveupPartitions(Set<Integer> set, JobContext ctx, Iterable<Integer> pIds,
      JobConfig cfg) {
    for (Integer pId : pIds) {
      if (isTaskGivenup(ctx, cfg, pId)) {
        set.add(pId);
      }
    }
  }

  private static List<Integer> getNextPartitions(SortedSet<Integer> candidatePartitions,
      Set<Integer> excluded, int n) {
    List<Integer> result = new ArrayList<Integer>();
    for (Integer pId : candidatePartitions) {
      if (result.size() >= n) {
        break;
      }

      if (!excluded.contains(pId)) {
        result.add(pId);
      }
    }

    return result;
  }

  private static void markPartitionDelayed(JobConfig cfg, JobContext ctx, int p) {
    long delayInterval = cfg.getTaskRetryDelay();
    if (delayInterval <= 0) {
      return;
    }
    long nextStartTime = ctx.getPartitionFinishTime(p) + delayInterval;
    ctx.setNextRetryTime(p, nextStartTime);
  }

  private static void markPartitionCompleted(JobContext ctx, int pId) {
    ctx.setPartitionState(pId, TaskPartitionState.COMPLETED);
    ctx.setPartitionFinishTime(pId, System.currentTimeMillis());
    ctx.incrementNumAttempts(pId);
  }

  private static void markPartitionError(JobContext ctx, int pId, TaskPartitionState state,
      boolean incrementAttempts) {
    ctx.setPartitionState(pId, state);
    ctx.setPartitionFinishTime(pId, System.currentTimeMillis());
    if (incrementAttempts) {
      ctx.incrementNumAttempts(pId);
    }
  }

  private static void markAllPartitionsError(JobContext ctx, TaskPartitionState state,
      boolean incrementAttempts) {
    for (int pId : ctx.getPartitionSet()) {
      markPartitionError(ctx, pId, state, incrementAttempts);
    }
  }

  /**
   * Return the assignment of task partitions per instance.
   */
  private static Map<String, SortedSet<Integer>> getTaskPartitionAssignments(
      Iterable<String> instanceList, ResourceAssignment assignment, Set<Integer> includeSet) {
    Map<String, SortedSet<Integer>> result = new HashMap<String, SortedSet<Integer>>();
    for (String instance : instanceList) {
      result.put(instance, new TreeSet<Integer>());
    }

    for (Partition partition : assignment.getMappedPartitions()) {
      int pId = TaskUtil.getPartitionId(partition.getPartitionName());
      if (includeSet.contains(pId)) {
        Map<String, String> replicaMap = assignment.getReplicaMap(partition);
        for (String instance : replicaMap.keySet()) {
          SortedSet<Integer> pList = result.get(instance);
          if (pList != null) {
            pList.add(pId);
          }
        }
      }
    }
    return result;
  }

  private static Set<Integer> getNonReadyPartitions(JobContext ctx, long now) {
    Set<Integer> nonReadyPartitions = Sets.newHashSet();
    for (int p : ctx.getPartitionSet()) {
      long toStart = ctx.getNextRetryTime(p);
      if (now < toStart) {
        nonReadyPartitions.add(p);
      }
    }
    return nonReadyPartitions;
  }

  private TaskAssignmentCalculator getAssignmentCalulator(JobConfig jobConfig) {
    Map<String, TaskConfig> taskConfigMap = jobConfig.getTaskConfigMap();
    if (taskConfigMap != null && !taskConfigMap.isEmpty()) {
      return genericTaskAssignmentCal;
    } else {
      return fixTaskAssignmentCal;
    }
  }

  /**
   * Computes the partition name given the resource name and partition id.
   */
  private String pName(String resource, int pId) {
    return resource + "_" + pId;
  }

  /**
   * An (instance, state) pair.
   */
  private static class PartitionAssignment {
    private final String _instance;
    private final String _state;

    private PartitionAssignment(String instance, String state) {
      _instance = instance;
      _state = state;
    }
  }
}
