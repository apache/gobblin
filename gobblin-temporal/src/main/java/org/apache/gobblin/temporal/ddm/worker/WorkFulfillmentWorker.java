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

package org.apache.gobblin.temporal.ddm.worker;

import com.typesafe.config.Config;
import io.temporal.client.WorkflowClient;
import org.apache.gobblin.temporal.cluster.AbstractTemporalWorker;
import org.apache.gobblin.temporal.ddm.activity.impl.ProcessWorkUnitImpl;
import org.apache.gobblin.temporal.ddm.workflow.impl.NestingExecOfProcessWorkUnitWorkflowImpl;
import org.apache.gobblin.temporal.ddm.workflow.impl.ProcessWorkUnitsWorkflowImpl;


/** Worker for the {@link ProcessWorkUnitsWorkflowImpl} super-workflow */
public class WorkFulfillmentWorker extends AbstractTemporalWorker {
    public WorkFulfillmentWorker(Config config, WorkflowClient workflowClient) {
        super(config, workflowClient);
    }

    @Override
    protected Class<?>[] getWorkflowImplClasses() {
        return new Class[] { ProcessWorkUnitsWorkflowImpl.class, NestingExecOfProcessWorkUnitWorkflowImpl.class };
    }

    @Override
    protected Object[] getActivityImplInstances() {
        return new Object[] { new ProcessWorkUnitImpl() };
    }
}
