package com.linkedin.uif.qualitychecker;

import java.util.List;

/**
 * PolicyChecker takes in a list of Policy objects
 * executes each one, and then stores the output
 * in a PolicyCheckResults object
 */
public class PolicyChecker
{
    private final List<Policy> list;
    
    public PolicyChecker(List<Policy> list) {
        this.list = list;
    }
        
    public PolicyCheckResults executePolicies() {
        PolicyCheckResults results = new PolicyCheckResults();
        for (Policy p : this.list) {
            results.getPolicyResults().put(p.executePolicy(), p.getType());
        }
        return results;
    }
}