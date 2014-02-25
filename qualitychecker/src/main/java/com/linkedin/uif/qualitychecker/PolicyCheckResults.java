package com.linkedin.uif.qualitychecker;

import java.util.HashMap;
import java.util.Map;

/**
 * Wrapper around a Map of PolicyResults and Policy.Type
 */
public class PolicyCheckResults
{
    private final Map<QualityCheckResult, Policy.Type> results;
    
    public PolicyCheckResults() {
        this.results = new HashMap<QualityCheckResult, Policy.Type>();
    }
    
    public Map<QualityCheckResult, Policy.Type> getPolicyResults() {
        return this.results;
    }
    
    public void add(QualityCheckResult status, Policy.Type type) {
        this.results.put(status, type);
    }
}
