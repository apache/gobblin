package com.linkedin.uif.publisher;

import com.linkedin.uif.configuration.MetaStoreClient;
import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.qualitychecker.PolicyCheckResults;

public class TaskPublisherBuilderFactory
{
    public TaskPublisherBuilder newTaskPublisherBuilder(WorkUnitState workUnitState, PolicyCheckResults results, MetaStoreClient collector) {
        return new TaskPublisherBuilder(workUnitState, results, collector);
    }
}
