package org.apache.gobblin.temporal.cluster;

import com.typesafe.config.Config;

import io.temporal.client.WorkflowClient;

import org.apache.gobblin.temporal.workflows.IllustrationTaskActivityImpl;
import org.apache.gobblin.temporal.workflows.NestingExecWorkflowImpl;


public class NestingExecWorker extends AbstractTemporalWorker {
    public NestingExecWorker(Config config, WorkflowClient workflowClient, String queueName) {
        super(config, workflowClient, queueName);
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
