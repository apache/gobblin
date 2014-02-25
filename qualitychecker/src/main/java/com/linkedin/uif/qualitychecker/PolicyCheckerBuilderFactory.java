package com.linkedin.uif.qualitychecker;

import com.linkedin.uif.configuration.MetaStoreClient;
import com.linkedin.uif.scheduler.TaskState;

public class PolicyCheckerBuilderFactory
{
    public PolicyCheckerBuilder newPolicyCheckerBuilder(TaskState taskState, MetaStoreClient collector) {
        return new PolicyCheckerBuilder(taskState, collector);
    }
}
