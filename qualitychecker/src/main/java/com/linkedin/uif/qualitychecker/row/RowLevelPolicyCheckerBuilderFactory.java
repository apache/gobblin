package com.linkedin.uif.qualitychecker.row;

import com.linkedin.uif.configuration.State;

public class RowLevelPolicyCheckerBuilderFactory
{
    public RowLevelPolicyCheckerBuilder newPolicyCheckerBuilder(State state) {
        return new RowLevelPolicyCheckerBuilder(state);
    }
}
