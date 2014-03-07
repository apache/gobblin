package com.linkedin.uif.qualitychecker;

import java.util.HashMap;
import java.util.Map;

/**
 * Wrapper around a Map of PolicyResults and Policy.Type
 */
public class PolicyCheckResults
{
    private final Map<Policy.Result, Policy.Type> results;
    
    public PolicyCheckResults() {
        this.results = new HashMap<Policy.Result, Policy.Type>();
    }
    
    public Map<Policy.Result, Policy.Type> getPolicyResults() {
        return this.results;
    }
}