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

package org.apache.gobblin.temporal.ddm.workflow;

import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

import org.apache.gobblin.temporal.ddm.work.CommitStats;
import org.apache.gobblin.temporal.ddm.work.WUProcessingSpec;


/**
 * Workflow for committing the output of work done by {@link org.apache.gobblin.temporal.ddm.activity.impl.ProcessWorkUnitImpl}
 */
@WorkflowInterface
public interface CommitStepWorkflow {

    /**
     * Commit the output of the work done by {@link org.apache.gobblin.temporal.ddm.activity.impl.ProcessWorkUnitImpl}
     * @return number of workunits committed
     */
    @WorkflowMethod
    CommitStats commit(WUProcessingSpec workSpec);
}
