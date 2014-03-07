package com.linkedin.uif.qualitychecker;

import com.linkedin.uif.configuration.State;

public class PolicyCheckerBuilderFactory
{
    public PolicyCheckerBuilder newPolicyCheckerBuilder(State state) {
        return new PolicyCheckerBuilder(state);
    }
}
