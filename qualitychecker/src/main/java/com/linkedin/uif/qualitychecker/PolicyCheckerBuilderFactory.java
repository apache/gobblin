package com.linkedin.uif.qualitychecker;

import com.linkedin.uif.configuration.MetaStoreClient;
import com.linkedin.uif.configuration.WorkUnitState;

public class PolicyCheckerBuilderFactory
{
    public PolicyCheckerBuilder newPolicyCheckerBuilder(WorkUnitState workUnitState, MetaStoreClient collector) {
        return new PolicyCheckerBuilder(workUnitState, collector);
    }
}
