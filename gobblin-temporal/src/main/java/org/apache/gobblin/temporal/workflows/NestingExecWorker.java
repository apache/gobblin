package org.apache.gobblin.temporal.workflows;

import io.temporal.client.WorkflowClient;
import org.apache.gobblin.temporal.cluster.AbstractTemporalWorker;


public class NestingExecWorker extends AbstractTemporalWorker {
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
