package com.linkedin.uif.schema;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.SourceState;
import com.linkedin.uif.configuration.State;

public class WorkUnitSchemaRetriever implements SchemaRetriever
{    
    public String getOldSchema(State state) {
        SourceState sourceState = (SourceState) state;
        String oldSchema = null;

        for (State oldState : sourceState.getPreviousStates()) {            
            oldSchema = oldState.getProp(ConfigurationKeys.WRITER_OUTPUT_SCHEMA);
            if (oldSchema != null) {
                break;
            }
        }
        return oldSchema;
    }   
}
