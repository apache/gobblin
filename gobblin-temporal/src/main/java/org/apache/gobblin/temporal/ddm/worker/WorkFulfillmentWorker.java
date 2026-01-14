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
import org.apache.gobblin.temporal.ddm.activity.impl.CommitActivityImpl;
import org.apache.gobblin.temporal.ddm.activity.impl.DeleteWorkDirsActivityImpl;
import org.apache.gobblin.temporal.ddm.activity.impl.EmitOTelMetricsImpl;
import org.apache.gobblin.temporal.ddm.activity.impl.GenerateWorkUnitsImpl;
import org.apache.gobblin.temporal.ddm.activity.impl.ProcessWorkUnitImpl;
import org.apache.gobblin.temporal.ddm.activity.impl.RecommendScalingForWorkUnitsLinearHeuristicImpl;
import org.apache.gobblin.temporal.ddm.workflow.impl.CommitStepWorkflowImpl;
import org.apache.gobblin.temporal.ddm.workflow.impl.ExecuteGobblinWorkflowImpl;
import org.apache.gobblin.temporal.ddm.workflow.impl.GenerateWorkUnitsWorkflowImpl;
import org.apache.gobblin.temporal.ddm.workflow.impl.NestingExecOfProcessWorkUnitWorkflowImpl;
import org.apache.gobblin.temporal.ddm.workflow.impl.ProcessWorkUnitsWorkflowImpl;
import org.apache.gobblin.temporal.workflows.metrics.SubmitGTEActivityImpl;
import org.apache.gobblin.util.ConfigUtils;


/** Worker for the {@link ProcessWorkUnitsWorkflowImpl} super-workflow */
public class WorkFulfillmentWorker extends AbstractTemporalWorker {
    public static final long DEADLOCK_DETECTION_TIMEOUT_SECONDS = 120; // TODO: make configurable!
    private final int maxConcurrentActivityExecutionSize;
    private final int maxConcurrentLocalActivityExecutionSize;
    private final int maxConcurrentWorkflowTaskExecutionSize;

    public WorkFulfillmentWorker(Config config, WorkflowClient workflowClient) {
        super(config, workflowClient);
        int defaultThreadsPerWorker = GobblinTemporalConfigurationKeys.DEFAULT_TEMPORAL_NUM_THREADS_PER_WORKER;

        // Fallback chain: TEMPORAL_NUM_THREADS_PER_WORKER -> DEFAULT
        int workerThreads = ConfigUtils.getInt(config,
            GobblinTemporalConfigurationKeys.TEMPORAL_NUM_THREADS_PER_WORKER,
            defaultThreadsPerWorker);

        this.maxConcurrentActivityExecutionSize = ConfigUtils.getInt(config,
            GobblinTemporalConfigurationKeys.TEMPORAL_MAX_CONCURRENT_ACTIVITY_SIZE,
            workerThreads);
        this.maxConcurrentLocalActivityExecutionSize = ConfigUtils.getInt(config,
            GobblinTemporalConfigurationKeys.TEMPORAL_MAX_CONCURRENT_LOCAL_ACTIVITY_SIZE,
            workerThreads);
        this.maxConcurrentWorkflowTaskExecutionSize = ConfigUtils.getInt(config,
            GobblinTemporalConfigurationKeys.TEMPORAL_MAX_CONCURRENT_WORKFLOW_TASK_SIZE,
            workerThreads);
    }

    @Override
    protected Class<?>[] getWorkflowImplClasses() {
        return new Class[] { ExecuteGobblinWorkflowImpl.class, ProcessWorkUnitsWorkflowImpl.class, NestingExecOfProcessWorkUnitWorkflowImpl.class,
            CommitStepWorkflowImpl.class, GenerateWorkUnitsWorkflowImpl.class };
    }

    @Override
    protected Object[] getActivityImplInstances() {
        return new Object[] { new SubmitGTEActivityImpl(), new GenerateWorkUnitsImpl(), new RecommendScalingForWorkUnitsLinearHeuristicImpl(), new ProcessWorkUnitImpl(),
            new CommitActivityImpl(), new DeleteWorkDirsActivityImpl(), new EmitOTelMetricsImpl()};
    }

    @Override
    protected WorkerOptions createWorkerOptions() {
        return WorkerOptions.newBuilder()
            // default is only 1s - WAY TOO SHORT for `o.a.hadoop.fs.FileSystem#listStatus`!
            .setDefaultDeadlockDetectionTimeout(TimeUnit.SECONDS.toMillis(DEADLOCK_DETECTION_TIMEOUT_SECONDS))
            .setMaxConcurrentActivityExecutionSize(this.maxConcurrentActivityExecutionSize)
            .setMaxConcurrentLocalActivityExecutionSize(this.maxConcurrentLocalActivityExecutionSize)
            .setMaxConcurrentWorkflowTaskExecutionSize(this.maxConcurrentWorkflowTaskExecutionSize)
            .build();
    }
}
