package com.linkedin.uif.qualitychecker;

import com.linkedin.uif.configuration.State;
import com.linkedin.uif.configuration.WorkUnitState;

/**
 * Policy takes in a TaskState (Task metadata)
 * and a policy type
 */
public abstract class Policy
{   
    private final State state;
    private final Type type;
    
    public enum Type {
        MANDATORY,     // The test is mandatory
        OPTIONAL       // The test is optional
    };
    
    public enum Result {
      PASSED,          // The test passed
      FAILED           // The test failed
    };
    
    public Policy(State state, Policy.Type type) {
        this.state = state;
        this.type = type;
    }
    
    /**
     * Main method that defines the semantics of this policy
     * This method will be executed by the PolicyChecker
     */
    public abstract Result executePolicy();

    public State getTaskState()
    {
        return state;
    }

    public Type getType()
    {
        return type;
    }
    
    public State getPreviousTableState()
    {
        WorkUnitState workUnitState = (WorkUnitState) state;
        return workUnitState.getPreviousTableState();
    }
    
    @Override
    public String toString() {
        return this.getClass().getName();
    }
}
