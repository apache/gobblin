package com.linkedin.uif.qualitychecker;

import com.linkedin.uif.configuration.MetaStoreClient;
import com.linkedin.uif.scheduler.TaskState;

/**
 * Policy takes in a TaskState (Task metadata), a
 * MetaStoreClient (external metadata), and a
 * policy type
 */
public abstract class Policy
{   
    private final TaskState taskState;
    private final MetaStoreClient metadata;
    private Type type;
    private PolicyResult result;
    
    public enum Type {
        MANDATORY,     // The test is mandatory
        OPTIONAL       // The test is optional
    };
    
    public Policy(TaskState taskState, MetaStoreClient metadata, Type type) {
        this.taskState = taskState;
        this.metadata = metadata;
        this.setType(type);
        this.setResult(PolicyResult.FAILED);
    }
    
    /**
     * Main method that defines the semantics of this policy
     * This method will be executed by the PolicyChecker
     */
    public abstract PolicyResult executePolicy();

    public TaskState getTaskState()
    {
        return taskState;
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

    public PolicyResult getResult()
    {
        return result;
    }

    public void setResult(PolicyResult result)
    {
        this.result = result;
    }
}
