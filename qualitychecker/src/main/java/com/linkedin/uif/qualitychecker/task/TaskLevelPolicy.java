package com.linkedin.uif.qualitychecker.task;

import com.linkedin.uif.configuration.State;
import com.linkedin.uif.configuration.WorkUnitState;

public abstract class TaskLevelPolicy
{   
    private final State state;
    private final Type type;
    
    public enum Type {
        FAIL,          // Fail if the test does not pass
        OPTIONAL       // The test is optional
    };
    
    public enum Result {
      PASSED,          // The test passed
      FAILED           // The test failed
    };
    
    public TaskLevelPolicy(State state, TaskLevelPolicy.Type type) {
        this.state = state;
        this.type = type;
    }

    public State getTaskState()
    {
        return state;
    }

    public Type getType()
    {
        return type;
    }
    
    public abstract Result executePolicy();
        
    @Override
    public String toString() {
        return this.getClass().getName();
    }

    public State getPreviousTableState()
    {
        WorkUnitState workUnitState = (WorkUnitState) this.state;
        return workUnitState.getPreviousTableState();
    }
}
