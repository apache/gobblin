package com.linkedin.uif.qualitychecker;

import com.linkedin.uif.configuration.State;
import com.linkedin.uif.qualitychecker.task.TaskLevelPolicy;

public class TestTaskLevelPolicy extends TaskLevelPolicy
{
    public TestTaskLevelPolicy(State state, Type type)
    {
        super(state, type);
    }

    @Override
    public Result executePolicy()
    {
        return Result.PASSED;
    }
}