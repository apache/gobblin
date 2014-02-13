package com.linkedin.uif.qualitychecker;

import java.util.ArrayList;

/**
 * Wrapper around an ArrayList of PolicyResults
 */
public class PolicyCheckResults
{
    private ArrayList<PolicyResult> results;
    
    public PolicyCheckResults() {
        this.results = new ArrayList<PolicyResult>();
    }
    
    public ArrayList<PolicyResult> getPolicyResults() {
        return this.results;
    }
    
    @Override
    public String toString() {
        StringBuilder resultsString = new StringBuilder();
        for (PolicyResult result : results) {
            resultsString.append(result.toString() + ",");
        }
        return resultsString.toString();
    }
}
