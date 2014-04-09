package com.linkedin.uif.qualitychecker.task;

import java.util.HashMap;
import java.util.Map;

/**
 * Wrapper around a Map of PolicyResults and Policy.Type
 */
public class TaskLevelPolicyCheckResults
{
    private final Map<TaskLevelPolicy.Result, TaskLevelPolicy.Type> results;
    
    public TaskLevelPolicyCheckResults() {
        this.results = new HashMap<TaskLevelPolicy.Result, TaskLevelPolicy.Type>();
    }
    
    public Map<TaskLevelPolicy.Result, TaskLevelPolicy.Type> getPolicyResults() {
        return this.results;
    }
}