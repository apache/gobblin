package com.linkedin.uif.qualitychecker;

import com.linkedin.uif.configuration.MetaStoreClient;
import com.linkedin.uif.configuration.State;

public class PolicyCheckerBuilderFactory
{
    public PolicyCheckerBuilder newPolicyCheckerBuilder(State state, MetaStoreClient collector) {
        return new PolicyCheckerBuilder(state, collector);
    }
}
