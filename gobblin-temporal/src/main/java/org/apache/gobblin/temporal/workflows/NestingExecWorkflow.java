package org.apache.gobblin.temporal.workflows;

import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;
import java.util.Optional;

/**
 * Process all `TASK`s of `workload`, from `startIndex` to the end by creating child workflows, where this and
 * descendants should have at most `maxBranchesPerTree`, with at most `maxSubTreesPerTree` of those being child
 * workflows.  (Non-child-workflow branches being activities.)
 *
 * IMPORTANT: `Math.sqrt(maxBranchesPerTree) == maxSubTreesPerTree` provides a good rule-of-thumb; `maxSubTreesPerTree
 * should not exceed that.
 *
 * @param <TASK> the type of task for which to invoke an appropriate activity
 * @param maxSubTreesForCurrentTreeOverride when the current tree should use different max sub-trees than descendants
 */

@WorkflowInterface
public interface NestingExecWorkflow<TASK> {
    @WorkflowMethod
    int performWork(
            WFAddr addr,
            Workload<TASK> workload,
            int startIndex,
            int maxBranchesPerTree,
            int maxSubTreesPerTree,
            Optional<Integer> maxSubTreesForCurrentTreeOverride
    );
}
