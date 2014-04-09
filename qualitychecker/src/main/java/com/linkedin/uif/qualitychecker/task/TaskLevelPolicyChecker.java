package com.linkedin.uif.qualitychecker.task;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PolicyChecker takes in a list of Policy objects
 * executes each one, and then stores the output
 * in a PolicyCheckResults object
 */
public class TaskLevelPolicyChecker
{
    private final List<TaskLevelPolicy> list;
    private static final Logger LOG = LoggerFactory.getLogger(TaskLevelPolicyChecker.class);
    
    public TaskLevelPolicyChecker(List<TaskLevelPolicy> list) {
        this.list = list;
    }
        
    public TaskLevelPolicyCheckResults executePolicies() {
        TaskLevelPolicyCheckResults results = new TaskLevelPolicyCheckResults();
        for (TaskLevelPolicy p : this.list) {
            TaskLevelPolicy.Result result = p.executePolicy();
            results.getPolicyResults().put(result, p.getType());
            LOG.info("TaskLevelPolicy " + p + " of type " + p.getType() + " executed with result " + result);
        }
        return results;
    }
}