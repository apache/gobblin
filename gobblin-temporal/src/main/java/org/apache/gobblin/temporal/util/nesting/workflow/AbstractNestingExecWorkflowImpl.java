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

package org.apache.gobblin.temporal.util.nesting.workflow;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.commons.compress.utils.Lists;

import io.temporal.api.enums.v1.ParentClosePolicy;
import io.temporal.workflow.Async;
import io.temporal.workflow.ChildWorkflowOptions;
import io.temporal.workflow.Promise;
import io.temporal.workflow.Workflow;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.temporal.ddm.util.TemporalWorkFlowUtils;
import org.apache.gobblin.temporal.util.nesting.work.WorkflowAddr;
import org.apache.gobblin.temporal.util.nesting.work.Workload;


/** Core skeleton of {@link NestingExecWorkflow}: realizing classes need only define {@link #launchAsyncActivity} */
@Slf4j
public abstract class AbstractNestingExecWorkflowImpl<WORK_ITEM, ACTIVITY_RESULT> implements NestingExecWorkflow<WORK_ITEM> {
  public static final int NUM_SECONDS_TO_PAUSE_BEFORE_CREATING_SUB_TREE_DEFAULT = 10;
  public static final int MAX_CHILD_SUB_TREE_LEAVES_BEFORE_SHOULD_PAUSE_DEFAULT = 100;

  @Override
  public int performWorkload(
      final WorkflowAddr addr,
      final Workload<WORK_ITEM> workload,
      final int startIndex,
      final int maxBranchesPerTree,
      final int maxSubTreesPerTree,
      final Optional<Integer> maxSubTreesForCurrentTreeOverride
  ) {
    final int maxSubTreesForCurrent = maxSubTreesForCurrentTreeOverride.orElse(maxSubTreesPerTree);
    final int maxLeaves = maxBranchesPerTree - maxSubTreesForCurrent;
    final Optional<Workload.WorkSpan<WORK_ITEM>> optSpan = workload.getSpan(startIndex, maxLeaves);
    log.info("[" + addr + "] " + workload + " w/ start '" + startIndex + "'"
        + "; tree (" + maxBranchesPerTree + "/" + maxSubTreesPerTree + "): " + optSpan);
    if (!optSpan.isPresent()) {
      return 0;
    } else {
      final Workload.WorkSpan<WORK_ITEM> workSpan = optSpan.get();
      final Iterable<WORK_ITEM> iterable = () -> workSpan;
      final List<Promise<ACTIVITY_RESULT>> childActivities = StreamSupport.stream(iterable.spliterator(), false)
          .map(t -> launchAsyncActivity(t))
          .collect(Collectors.toList());
      final List<Promise<Integer>> childSubTrees = new ArrayList<>();
      if (workSpan.getNumElems() == maxLeaves) { // received as many as requested (did not stop short)
        int subTreeId = 0;
        for (int subTreeChildMaxSubTreesPerTree
            : consolidateSubTreeGrandChildren(maxSubTreesForCurrent, maxBranchesPerTree, maxSubTreesPerTree)) {
          // CAUTION: calc these *before* incrementing `subTreeId`!
          final int childStartIndex = startIndex + maxLeaves + (maxBranchesPerTree * subTreeId);
          final int nextChildId = maxLeaves + subTreeId;
          final WorkflowAddr childAddr = addr.createChild(nextChildId);
          final NestingExecWorkflow<WORK_ITEM> child = createChildWorkflow(childAddr);
          if (!workload.isIndexKnownToExceed(childStartIndex)) { // best-effort short-circuiting
            // IMPORTANT: insert pause before launch of each child workflow that may have direct leaves of its own.  periodic pauses spread the load on the
            // temporal server, to avoid a sustained burst from submitting potentially very many async activities over the full hierarchical elaboration
            final int numDirectLeavesChildMayHave = maxBranchesPerTree - subTreeChildMaxSubTreesPerTree;
            if (numDirectLeavesChildMayHave > 0) {
              Workflow.sleep(calcPauseDurationBeforeCreatingSubTree(numDirectLeavesChildMayHave));
            }
            childSubTrees.add(
                Async.function(child::performWorkload, childAddr, workload, childStartIndex, maxBranchesPerTree,
                    maxSubTreesPerTree, Optional.of(subTreeChildMaxSubTreesPerTree)));
            ++subTreeId;
          }
        }
      }
      final Promise<Void> allActivityChildren = Promise.allOf(childActivities);
      allActivityChildren.get(); // ensure all complete prior to counting them in `overallActivitiesRollupCount`
      // TODO: determine whether any benefit to unordered `::get` blocking for any next ready (perhaps no difference...)
      final int descendantActivitiesRollupCount = childSubTrees.stream().map(Promise::get).reduce(0, (x, y) -> x + y);
      // TODO: consider a generalized reduce op for things other than counting!
      final int overallActivitiesRollupCount = workSpan.getNumElems() + descendantActivitiesRollupCount;
      log.info("[" + addr + "] activites finished coordinating: " + overallActivitiesRollupCount);
      return overallActivitiesRollupCount;
    }
  }

  /** Factory for invoking the specific activity by providing it args via {@link Async::function} */
  protected abstract Promise<ACTIVITY_RESULT> launchAsyncActivity(WORK_ITEM task);

  protected NestingExecWorkflow<WORK_ITEM> createChildWorkflow(final WorkflowAddr childAddr) {
    // preserve the current workflow ID of this parent, but add the (hierarchical) address extension specific to each child
    String thisWorkflowId = Workflow.getInfo().getWorkflowId();
    String childWorkflowId = thisWorkflowId.replaceAll("-[^-]+$", "") + "-" + childAddr;
    ChildWorkflowOptions childOpts = ChildWorkflowOptions.newBuilder()
        .setParentClosePolicy(ParentClosePolicy.PARENT_CLOSE_POLICY_TERMINATE)
        .setWorkflowId(childWorkflowId)
        .setSearchAttributes(TemporalWorkFlowUtils.convertSearchAttributesValuesFromListToObject(Workflow.getSearchAttributes()))
        .build();
    return Workflow.newChildWorkflowStub(NestingExecWorkflow.class, childOpts);
  }

  /** @return how long to pause prior to creating a child workflow, based on `numDirectLeavesChildMayHave` */
  protected Duration calcPauseDurationBeforeCreatingSubTree(int numDirectLeavesChildMayHave) {
    // (only pause when an appreciable number of leaves)
    // TODO: use a configuration value, for simpler adjustment, rather than hard-code
    return numDirectLeavesChildMayHave > MAX_CHILD_SUB_TREE_LEAVES_BEFORE_SHOULD_PAUSE_DEFAULT
        ? Duration.ofSeconds(NUM_SECONDS_TO_PAUSE_BEFORE_CREATING_SUB_TREE_DEFAULT)
        : Duration.ZERO;
  }

  /**
   * "right-tilt" sub-tree's grandchildren, so final child gets all grandchildren (vs. constant grandchildren/child)
   *   i.e. NOT!:
   *     List<Integer> naiveUniformity = Collections.nCopies(numSubTreesPerSubTree, numSubTreeChildren);
   * @return each sub-tree's desired size, in ascending sub-tree order
   */
  protected static List<Integer> consolidateSubTreeGrandChildren(
      final int numSubTreesPerSubTree,
      final int numChildrenTotal,
      final int numSubTreeChildren
  ) {
    if (numSubTreesPerSubTree <= 0) {
      return Lists.newArrayList();
    } else if (isSqrt(numSubTreeChildren, numChildrenTotal)) {
      // redistribute all grandchild sub-trees to pack every grandchild beneath the final child sub-tree
      final List<Integer> grandChildCounts = new ArrayList<>(Collections.nCopies(numSubTreesPerSubTree - 1, 0));
      grandChildCounts.add(numChildrenTotal);
      return grandChildCounts;
    } else {
      final int totalGrandChildSubTrees = numSubTreesPerSubTree * numSubTreeChildren;
      final int numTreesWithSolelySubTreeBranches = totalGrandChildSubTrees / numChildrenTotal;
      final int numSubTreesRemaining = totalGrandChildSubTrees % numChildrenTotal;
      assert (numTreesWithSolelySubTreeBranches == 1 && numSubTreesRemaining == 0) || numTreesWithSolelySubTreeBranches == 0
          : "present limitation: at most one sub-tree may use further branching: (found: numSubTreesPerSubTree: "
          + numSubTreesPerSubTree + "; numChildrenTotal: " + numChildrenTotal + " / numSubTreeChildren: "
          + numSubTreeChildren + ")";
      final List<Integer> grandChildCounts = new ArrayList<>(Collections.nCopies(numSubTreesPerSubTree - (numTreesWithSolelySubTreeBranches + 1), 0));
      grandChildCounts.addAll(Collections.nCopies(Math.min(1, numSubTreesPerSubTree - numTreesWithSolelySubTreeBranches), numSubTreesRemaining));
      grandChildCounts.addAll(Collections.nCopies(Math.min(numTreesWithSolelySubTreeBranches, numSubTreesPerSubTree), numChildrenTotal));
      return grandChildCounts;
    }
  }

  /** @return whether `maxSubTrees` == `Math.sqrt(maxBranches)` */
  private static boolean isSqrt(int maxSubTrees, int maxBranches) {
    return maxSubTrees > 0 && maxSubTrees * maxSubTrees == maxBranches;
  }
}
