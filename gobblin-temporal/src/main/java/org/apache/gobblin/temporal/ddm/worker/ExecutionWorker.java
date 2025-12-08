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

import java.util.concurrent.TimeUnit;

import com.typesafe.config.Config;

import io.temporal.client.WorkflowClient;
import io.temporal.worker.WorkerOptions;

import org.apache.gobblin.temporal.GobblinTemporalConfigurationKeys;
import org.apache.gobblin.temporal.cluster.AbstractTemporalWorker;
import org.apache.gobblin.temporal.ddm.activity.impl.ProcessWorkUnitImpl;
import org.apache.gobblin.temporal.ddm.workflow.impl.NestingExecOfProcessWorkUnitWorkflowImpl;
import org.apache.gobblin.temporal.ddm.workflow.impl.ProcessWorkUnitsWorkflowImpl;
import org.apache.gobblin.util.ConfigUtils;


/**
 * Specialized worker for Work Execution stage.
 * This worker only registers activities for:
 * - ProcessWorkUnit (Work Execution)
 * 
 * Runs on containers with stage-specific memory for work execution operations.
 * Polls the execution task queue to ensure activities run on appropriately-sized containers.
 */
public class ExecutionWorker extends AbstractTemporalWorker {
    public static final long DEADLOCK_DETECTION_TIMEOUT_SECONDS = 120;
    public int maxExecutionConcurrency;

    public ExecutionWorker(Config config, WorkflowClient workflowClient) {
        super(config, workflowClient);
        this.maxExecutionConcurrency = ConfigUtils.getInt(config, GobblinTemporalConfigurationKeys.TEMPORAL_NUM_THREADS_PER_WORKER,
            GobblinTemporalConfigurationKeys.DEFAULT_TEMPORAL_NUM_THREADS_PER_WORKER);
    }

    @Override
    protected Class<?>[] getWorkflowImplClasses() {
        return new Class[] { 
            ProcessWorkUnitsWorkflowImpl.class, 
            NestingExecOfProcessWorkUnitWorkflowImpl.class 
        };
    }

    @Override
    protected Object[] getActivityImplInstances() {
        // Only register activities for Execution stage
        return new Object[] { 
            new ProcessWorkUnitImpl()  // Work Execution only
        };
    }

    @Override
    protected WorkerOptions createWorkerOptions() {
        return WorkerOptions.newBuilder()
            .setDefaultDeadlockDetectionTimeout(TimeUnit.SECONDS.toMillis(DEADLOCK_DETECTION_TIMEOUT_SECONDS))
            .setMaxConcurrentActivityExecutionSize(this.maxExecutionConcurrency)
            .setMaxConcurrentLocalActivityExecutionSize(this.maxExecutionConcurrency)
            .setMaxConcurrentWorkflowTaskExecutionSize(this.maxExecutionConcurrency)
            .build();
    }

    @Override
    protected String getTaskQueue() {
        return ConfigUtils.getString(
            config,
            GobblinTemporalConfigurationKeys.EXECUTION_TASK_QUEUE,
            GobblinTemporalConfigurationKeys.DEFAULT_EXECUTION_TASK_QUEUE
        );
    }
}
