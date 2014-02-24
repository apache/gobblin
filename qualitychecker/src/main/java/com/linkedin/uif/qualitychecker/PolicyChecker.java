package com.linkedin.uif.qualitychecker;

/**
 * PolicyChecker takes in a list of Policy objects
 * executes each one, and then stores the output
 * in a PolicyCheckResults object
 */
public class PolicyChecker
{
    private final PolicyList list;
    
    public PolicyChecker(PolicyList list) {
        this.list = list;
    }
        
    public PolicyCheckResults executePolicies() {
        PolicyCheckResults results = new PolicyCheckResults();
        for (Policy p : this.list.getPolicyList()) {
            results.add(p.executePolicy(), p.getType());
        }
        return results;
    }
}