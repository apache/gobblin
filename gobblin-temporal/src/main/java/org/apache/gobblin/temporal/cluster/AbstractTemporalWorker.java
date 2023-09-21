package org.apache.gobblin.temporal.cluster;

import com.typesafe.config.Config;

import io.temporal.client.WorkflowClient;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;

import org.apache.gobblin.temporal.GobblinTemporalConfigurationKeys;
import org.apache.gobblin.util.ConfigUtils;


public abstract class AbstractTemporalWorker {
    private static final String
    private final WorkflowClient workflowClient;
    private final String queueName;
    private final WorkerFactory workerFactory;
    private final Config config;

    public AbstractTemporalWorker(Config cfg, WorkflowClient client) {
        config = cfg;
        workflowClient = client;
        queueName = ConfigUtils.getString(cfg,
            "task.queue.name"
            GobblinTemporalConfigurationKeys.DEFAULT_GOBBLIN_TEMPORAL_TASK_QUEUE);

        // Create a Worker factory that can be used to create Workers that poll specific Task Queues.
        workerFactory = WorkerFactory.newInstance(workflowClient);
    }

    public void start() {
        Worker worker = workerFactory.newWorker(queueName);
        // This Worker hosts both Workflow and Activity implementations.
        // Workflows are stateful, so you need to supply a type to create instances.
        worker.registerWorkflowImplementationTypes(getWorkflowImplClasses());
        // Activities are stateless and thread safe, so a shared instance is used.
        worker.registerActivitiesImplementations(getActivityImplInstances());
        // Start polling the Task Queue.
        workerFactory.start();
    }

    /**
     * Shuts down the worker.
     */
    public void shutdown() {
        workerFactory.shutdown();
    }

    /** @return workflow types for *implementation* classes (not interface) */
    protected abstract Class<?>[] getWorkflowImplClasses();

    /** @return activity instances; NOTE: activities must be stateless and thread-safe, so a shared instance is used. */
    protected abstract Object[] getActivityImplInstances();
}
