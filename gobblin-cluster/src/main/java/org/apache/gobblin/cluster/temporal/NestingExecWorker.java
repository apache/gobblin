package org.apache.gobblin.cluster.temporal;

import io.temporal.client.WorkflowClient;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerOptions;
import io.temporal.worker.WorkerFactory;
public class NestingExecWorker extends AbstractTemporalWorker{
    public NestingExecWorker(WorkflowClient workflowClient, String queueName) {
        super(workflowClient, queueName);
    }

    @Override
    protected Class<?>[] getWorkflowImplClasses() {
        return new Class[] { NestingExecWorkflowImpl.class };
    }

    @Override
    protected Object[] getActivityImplInstances() {
        return new Object[] { new IllustrationTaskActivityImpl() };
    }
}
