package com.linkedin.uif.qualitychecker;

import com.linkedin.uif.configuration.State;
import com.linkedin.uif.qualitychecker.row.RowLevelPolicy;

public class TestRowLevelPolicyFail extends RowLevelPolicy
{
    public TestRowLevelPolicyFail(State state, Type type)
    {
        super(state, type);
    }

    @Override
    public Result executePolicy(Object record)
    {
        return RowLevelPolicy.Result.PASSED;
    }
}
