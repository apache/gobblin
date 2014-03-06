package com.linkedin.uif.schema;

import java.util.Date;
import java.util.Map;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.SourceState;
import com.linkedin.uif.configuration.State;

public class WorkUnitSchemaRetriever implements SchemaRetriever
{
    private SourceState sourceState;
    
    @Override
    public boolean initialize(State state)
    {
        this.sourceState = (SourceState) state;
        return true;
    }

    @Override
    public String getLatestPreviousSchema()
    {
        String oldSchema = null;

        for (State oldState : this.sourceState.getPreviousStates()) {
            oldSchema = oldState.getProp(ConfigurationKeys.EXTRACT_SCHEMA);
            if (oldSchema != null) {
                break;
            }
        }
        return oldSchema;
    }

    @Override
    public Map<Date, String> getAllPreviousSchema()
    {
        return null;
    }

    @Override
    public boolean close()
    {
        return true;
    }   
}
