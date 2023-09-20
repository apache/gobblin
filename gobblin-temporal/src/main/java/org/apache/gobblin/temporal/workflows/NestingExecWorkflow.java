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
