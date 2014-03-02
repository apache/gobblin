package com.linkedin.uif.schema;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.SourceState;
import com.linkedin.uif.configuration.State;

public class WorkUnitSchemaRetriever implements SchemaRetriever
{
    
    private static final Log LOG = LogFactory.getLog(WorkUnitSchemaRetriever.class);
    
    public String getOldSchema(State state) {
        SourceState sourceState = (SourceState) state;
        String oldSchema = null;
        LOG.info("SIZE: " + sourceState.getPreviousStates().size());
        for (State oldState : sourceState.getPreviousStates()) {            
            oldSchema = oldState.getProp(ConfigurationKeys.WRITER_OUTPUT_SCHEMA);
            if (oldSchema != null) {
                break;
            }
        }
        return oldSchema;
    }   
}
