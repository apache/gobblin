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

package org.apache.gobblin.temporal.workflows;

import io.temporal.api.enums.v1.ParentClosePolicy;
import io.temporal.workflow.Async;
import io.temporal.workflow.ChildWorkflowOptions;
import io.temporal.workflow.Promise;
import io.temporal.workflow.Workflow;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.utils.Lists;

/** Core skeleton of {@link NestingExecWorkflow}: realizing classes need only define {@link #launchAsyncActivity} */
@Slf4j
public abstract class AbstractNestingExecWorkflowImpl<TASK, ACTIVITY_RESULT> implements NestingExecWorkflow<TASK> {
    @Override
    public int performWork(
            final WFAddr addr,
            final Workload<TASK> workload,
            final int startIndex,
            final int maxBranchesPerTree,
            final int maxSubTreesPerTree,
            final Optional<Integer> maxSubTreesForCurrentTreeOverride) {
        final int maxSubTreesForCurrent = maxSubTreesForCurrentTreeOverride.orElse(maxSubTreesPerTree);
        final int maxLeaves = maxBranchesPerTree - maxSubTreesForCurrent;
        final Optional<Workload.TaskSpan<TASK>> optSpan = workload.getSpan(startIndex, maxLeaves);
        log.info("[" + addr + "] " + workload + " w/ start '" + startIndex + "'" + "; tree (" + maxBranchesPerTree + "/" + maxSubTreesPerTree + "): " + optSpan);
        if (!optSpan.isPresent()) {
            return 0;
        } else {
            final Workload.TaskSpan<TASK> taskSpan = optSpan.get();
            final Iterable<TASK> iterable = () -> taskSpan;
            final List<Promise<ACTIVITY_RESULT>> childActivities = StreamSupport.stream(iterable.spliterator(), false)
                    .map(t -> launchAsyncActivity(t))
                    .collect(Collectors.toList());
            final List<Promise<Integer>> childSubTrees = new ArrayList<>();
            if (taskSpan.getNumElems() == maxLeaves) { // received as many as requested (did not stop short)
                int subTreeId = 0;
                for (int subTreeChildMaxSubTreesPerTree
                        : consolidateSubTreeGrandChildren(maxSubTreesForCurrent, maxBranchesPerTree, maxSubTreesPerTree)) {
                    // CAUTION: calc these *before* incrementing `subTreeId`!
                    final int childStartIndex = startIndex + maxLeaves + (maxBranchesPerTree * subTreeId);
                    final int nextChildId = maxLeaves + subTreeId;
                    final WFAddr childAddr = addr.createChild(nextChildId);
                    final NestingExecWorkflow<TASK> child = createChildWorkflow(childAddr);
                    if (!workload.isIndexKnownToExceed(childStartIndex)) { // best-effort short-circuiting
                        childSubTrees.add(
                                Async.function(child::performWork, childAddr, workload, childStartIndex, maxBranchesPerTree,
                                        maxSubTreesPerTree, Optional.of(subTreeChildMaxSubTreesPerTree)));
                        ++subTreeId;
                    }
                }
            }
            final Promise<Void> allActivityChildren = Promise.allOf(childActivities);
            allActivityChildren.get(); // ensure all complete prior to counting them in `overallActivitiesRollupCount`
            // TODO: determine whether any benefit to unordered `::get` blocking for any next ready (perhaps no difference...)
            final int descendantActivitiesRollupCount = childSubTrees.stream().map(Promise::get).reduce(0, (x, y) -> x + y);
            final int overallActivitiesRollupCount = taskSpan.getNumElems() + descendantActivitiesRollupCount;
            log.info("[" + addr + "] activites finished coordinating: " + overallActivitiesRollupCount);
            return overallActivitiesRollupCount;
        }
    }

    /** Factory for invoking the specific activity by providing it args via {@link Async::function} */
    protected abstract Promise<ACTIVITY_RESULT> launchAsyncActivity(TASK task);

    protected NestingExecWorkflow<TASK> createChildWorkflow(final WFAddr childAddr) {
        ChildWorkflowOptions childOpts = ChildWorkflowOptions.newBuilder()
                .setParentClosePolicy(ParentClosePolicy.PARENT_CLOSE_POLICY_ABANDON)
                .setWorkflowId("NestingExecWorkflow-" + childAddr)
                .build();
        return Workflow.newChildWorkflowStub(NestingExecWorkflow.class, childOpts);
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
