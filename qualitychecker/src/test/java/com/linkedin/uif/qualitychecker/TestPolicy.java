package com.linkedin.uif.qualitychecker;

import com.linkedin.uif.configuration.State;

public class TestPolicy extends Policy
{
    public TestPolicy(State state, Type type)
    {
        super(state, type);
    }

    @Override
    public Result executePolicy()
    {
        return Result.PASSED;
    }
}