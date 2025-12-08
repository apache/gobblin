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
import org.apache.gobblin.temporal.ddm.activity.impl.GenerateWorkUnitsImpl;
import org.apache.gobblin.temporal.ddm.activity.impl.RecommendScalingForWorkUnitsLinearHeuristicImpl;
import org.apache.gobblin.temporal.ddm.workflow.impl.CommitStepWorkflowImpl;
import org.apache.gobblin.temporal.ddm.workflow.impl.ExecuteGobblinWorkflowImpl;
import org.apache.gobblin.temporal.ddm.workflow.impl.GenerateWorkUnitsWorkflowImpl;
import org.apache.gobblin.temporal.workflows.metrics.SubmitGTEActivityImpl;
import org.apache.gobblin.util.ConfigUtils;


/**
 * Specialized worker for Work Discovery and Commit stages.
 * This worker registers activities for:
 * - GenerateWorkUnits (Work Discovery)
 * - RecommendScaling (lightweight scaling recommendation)
 * - CommitActivity (Commit)
 * - DeleteWorkDirs (Cleanup)
 *
 * Runs on containers with stage-specific memory for lightweight operations.
 * Polls the discovery/commit task queue to ensure activities run on appropriately-sized containers.
 */
public class DiscoveryCommitWorker extends AbstractTemporalWorker {
    public static final long DEADLOCK_DETECTION_TIMEOUT_SECONDS = 120;
    public int maxExecutionConcurrency;

    public DiscoveryCommitWorker(Config config, WorkflowClient workflowClient) {
        super(config, workflowClient);
        this.maxExecutionConcurrency = ConfigUtils.getInt(config, GobblinTemporalConfigurationKeys.TEMPORAL_NUM_THREADS_PER_WORKER,
            GobblinTemporalConfigurationKeys.DEFAULT_TEMPORAL_NUM_THREADS_PER_WORKER);
    }

    @Override
    protected Class<?>[] getWorkflowImplClasses() {
        return new Class[] {
            ExecuteGobblinWorkflowImpl.class,
            GenerateWorkUnitsWorkflowImpl.class,
            CommitStepWorkflowImpl.class
        };
    }

    @Override
    protected Object[] getActivityImplInstances() {
        // Register activities for both Discovery and Commit stages
        return new Object[] {
            new SubmitGTEActivityImpl(),
            new GenerateWorkUnitsImpl(),                          // Work Discovery
            new RecommendScalingForWorkUnitsLinearHeuristicImpl(), // Scaling recommendation
            new CommitActivityImpl(),                             // Commit
            new DeleteWorkDirsActivityImpl()                      // Cleanup
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
            GobblinTemporalConfigurationKeys.DISCOVERY_COMMIT_TASK_QUEUE,
            GobblinTemporalConfigurationKeys.DEFAULT_DISCOVERY_COMMIT_TASK_QUEUE
        );
    }
}
