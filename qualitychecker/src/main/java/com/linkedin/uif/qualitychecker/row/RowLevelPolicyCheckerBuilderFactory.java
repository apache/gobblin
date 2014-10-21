package com.linkedin.uif.qualitychecker.row;

import com.linkedin.uif.configuration.State;

public class RowLevelPolicyCheckerBuilderFactory
{
    public RowLevelPolicyCheckerBuilder newPolicyCheckerBuilder(State state, int index) {
        return RowLevelPolicyCheckerBuilder.newBuilder(state, index);
    }
}
