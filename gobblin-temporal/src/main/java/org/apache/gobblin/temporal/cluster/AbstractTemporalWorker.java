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

package org.apache.gobblin.temporal.cluster;

import java.util.Arrays;

import com.typesafe.config.Config;

import io.temporal.client.WorkflowClient;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;
import io.temporal.worker.WorkerOptions;

import org.apache.gobblin.temporal.GobblinTemporalConfigurationKeys;
import org.apache.gobblin.util.ConfigUtils;


/** Basic boilerplate for a {@link TemporalWorker} to register its activity and workflow capabilities and listen on a particular queue */
public abstract class AbstractTemporalWorker implements TemporalWorker {
    private final WorkflowClient workflowClient;
    private final String queueName;
    private final WorkerFactory workerFactory;
    private final Config config;

    public AbstractTemporalWorker(Config cfg, WorkflowClient client) {
        config = cfg;
        workflowClient = client;
        queueName = ConfigUtils.getString(cfg,
            GobblinTemporalConfigurationKeys.GOBBLIN_TEMPORAL_TASK_QUEUE,
            GobblinTemporalConfigurationKeys.DEFAULT_GOBBLIN_TEMPORAL_TASK_QUEUE);

        // Create a Worker factory that can be used to create Workers that poll specific Task Queues.
        workerFactory = WorkerFactory.newInstance(workflowClient);

        stashWorkerConfig(cfg);
    }

    @Override
    public void start() {
        Worker worker = workerFactory.newWorker(queueName, createWorkerOptions());
        // This Worker hosts both Workflow and Activity implementations.
        // Workflows are stateful, so you need to supply a type to create instances.
        worker.registerWorkflowImplementationTypes(getWorkflowImplClasses());
        // Activities are stateless and thread safe, so a shared instance is used.
        worker.registerActivitiesImplementations(getActivityImplInstances());
        // Start polling the Task Queue.
        workerFactory.start();
    }

    @Override
    public void shutdown() {
        workerFactory.shutdown();
    }

    protected WorkerOptions createWorkerOptions() {
        return null;
    }

    /** @return workflow types for *implementation* classes (not interface) */
    protected abstract Class<?>[] getWorkflowImplClasses();

    /** @return activity instances; NOTE: activities must be stateless and thread-safe, so a shared instance is used. */
    protected abstract Object[] getActivityImplInstances();

    private final void stashWorkerConfig(Config cfg) {
        // stash to associate with...
        WorkerConfig.forWorker(this.getClass(), cfg); // the worker itself
        Arrays.stream(getWorkflowImplClasses()).forEach(clazz -> WorkerConfig.withImpl(clazz, cfg)); // its workflow impls
        Arrays.stream(getActivityImplInstances()).forEach(obj -> WorkerConfig.withImpl(obj.getClass(), cfg)); // its activity impls
    }
}
