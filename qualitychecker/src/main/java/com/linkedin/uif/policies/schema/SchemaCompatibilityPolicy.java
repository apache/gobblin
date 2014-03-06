package com.linkedin.uif.policies.schema;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.State;
import com.linkedin.uif.qualitychecker.Policy;

public class SchemaCompatibilityPolicy extends Policy
{
    private State state;
    private State previousState;

    public SchemaCompatibilityPolicy(State state, Type type)
    {
        super(state, type);
        this.state = state;
        this.previousState = this.getPreviousTableState();
    }

    @Override
    public Result executePolicy()
    {
        // TODO how do you test for backwards compatibility?
        if (previousState.getProp(ConfigurationKeys.EXTRACT_SCHEMA) == null) {
            return Result.PASSED;
        }
        
        if (state.getProp(ConfigurationKeys.EXTRACT_SCHEMA).equals(previousState.getProp(ConfigurationKeys.EXTRACT_SCHEMA))) {
            return Result.PASSED;
        } else {
            return Result.FAILED;
        }
    }
}