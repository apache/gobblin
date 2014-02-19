package com.linkedin.uif.publisher;

import com.linkedin.uif.configuration.MetaStoreClient;
import com.linkedin.uif.qualitychecker.PolicyCheckResults;
import com.linkedin.uif.scheduler.TaskState;

public class TaskPublisherBuilderFactory
{
    public TaskPublisherBuilder newTaskPublisherBuilder(TaskState taskState, PolicyCheckResults results, MetaStoreClient collector) {
        return new TaskPublisherBuilder(taskState, results, collector);
    }
}
