package com.linkedin.uif.qualitychecker;

import com.linkedin.uif.configuration.MetaStoreClient;
import com.linkedin.uif.configuration.WorkUnitState;

/**
 * Policy takes in a TaskState (Task metadata), a
 * MetaStoreClient (external metadata), and a
 * policy type
 */
public abstract class Policy
{   
    private final WorkUnitState workUnitState;
    private final MetaStoreClient metadata;
    private Type type;
    private QualityCheckResult result;
    
    public enum Type {
        MANDATORY,     // The test is mandatory
        OPTIONAL       // The test is optional
    };
    
    public Policy(WorkUnitState workUnitState, MetaStoreClient metadata, Type type) {
        this.workUnitState = workUnitState;
        this.metadata = metadata;
        this.setType(type);
        this.setResult(QualityCheckResult.FAILED);
    }
    
    /**
     * Main method that defines the semantics of this policy
     * This method will be executed by the PolicyChecker
     */
    public abstract QualityCheckResult executePolicy();

    public WorkUnitState getTaskState()
    {
        return workUnitState;
    }

    public MetaStoreClient getMetadata()
    {
        return metadata;
    }

    public Type getType()
    {
        return type;
    }

    public void setType(Type type)
    {
        this.type = type;
    }

    public QualityCheckResult getResult()
    {
        return result;
    }

    public void setResult(QualityCheckResult result)
    {
        this.result = result;
    }
}
