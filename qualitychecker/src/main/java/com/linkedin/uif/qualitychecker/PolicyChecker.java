package com.linkedin.uif.qualitychecker;

import com.linkedin.uif.configuration.State;

/**
 * PolicyChecker takes in a list of Policy objects
 * executes each one, and then stores the output
 * in a State object
 */
public class PolicyChecker
{
    private PolicyList list;
    private State state;
    
    public PolicyChecker(PolicyList list) {
        this.list = list;
        this.state = new State();
    }
        
    public void checkAndWritePolicies() {
        PolicyCheckResults results = new PolicyCheckResults();
        for (Policy p : this.list.getPolicyList()) {
            results.getPolicyResults().add(p.executePolicy());
        }
        this.state.setProp("uif.qualitychecker.policyresults", results.toString());
    }
}