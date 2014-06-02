package com.linkedin.uif.policies.schema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.State;
import com.linkedin.uif.qualitychecker.task.TaskLevelPolicy;

public class SchemaCompatibilityPolicy extends TaskLevelPolicy
{
    private static final Logger log = LoggerFactory.getLogger(SchemaCompatibilityPolicy.class);
    
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
            log.info("Previous Task State does not contain a schema");
            return Result.PASSED;
        }
        
        if (state.getProp(ConfigurationKeys.EXTRACT_SCHEMA).equals(previousState.getProp(ConfigurationKeys.EXTRACT_SCHEMA))) {
            return Result.PASSED;
        } else {
            return Result.FAILED;
        }
    }
}