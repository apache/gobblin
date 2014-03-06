package com.linkedin.uif.test;

import com.linkedin.uif.configuration.State;
import com.linkedin.uif.qualitychecker.Policy;
import com.linkedin.uif.qualitychecker.Policy.Result;
import com.linkedin.uif.qualitychecker.Policy.Type;

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