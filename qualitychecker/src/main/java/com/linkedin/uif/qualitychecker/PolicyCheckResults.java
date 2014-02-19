package com.linkedin.uif.qualitychecker;

import java.util.HashMap;
import java.util.Map;

/**
 * Wrapper around a Map of PolicyResults and Policy.Type
 */
public class PolicyCheckResults
{
    private Map<PolicyResult, Policy.Type> results;
    
    public PolicyCheckResults() {
        this.results = new HashMap<PolicyResult, Policy.Type>();
    }
    
    public Map<PolicyResult, Policy.Type> getPolicyResults() {
        return this.results;
    }
    
    public void add(PolicyResult status, Policy.Type type) {
        this.results.put(status, type);
    }
}
