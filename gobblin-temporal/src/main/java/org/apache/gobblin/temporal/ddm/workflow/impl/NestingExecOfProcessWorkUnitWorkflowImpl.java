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

package org.apache.gobblin.temporal.ddm.workflow.impl;

import java.util.Properties;

import io.temporal.workflow.Async;
import io.temporal.workflow.Promise;
import io.temporal.workflow.Workflow;

import org.apache.gobblin.temporal.ddm.activity.ActivityType;
import org.apache.gobblin.temporal.ddm.activity.ProcessWorkUnit;
import org.apache.gobblin.temporal.ddm.work.WorkUnitClaimCheck;
import org.apache.gobblin.temporal.util.nesting.workflow.AbstractNestingExecWorkflowImpl;


/** {@link org.apache.gobblin.temporal.util.nesting.workflow.NestingExecWorkflow} for {@link ProcessWorkUnit} */
public class NestingExecOfProcessWorkUnitWorkflowImpl extends AbstractNestingExecWorkflowImpl<WorkUnitClaimCheck, Integer> {

  @Override
  protected Promise<Integer> launchAsyncActivity(final WorkUnitClaimCheck wu, final Properties props) {
    final ProcessWorkUnit processWorkUnitStub = Workflow.newActivityStub(ProcessWorkUnit.class,
        ActivityType.PROCESS_WORKUNIT.buildActivityOptions(props, true));
    return Async.function(processWorkUnitStub::processWorkUnit, wu);
  }
}
