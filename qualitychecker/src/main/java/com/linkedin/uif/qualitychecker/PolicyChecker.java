package com.linkedin.uif.qualitychecker;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PolicyChecker takes in a list of Policy objects
 * executes each one, and then stores the output
 * in a PolicyCheckResults object
 */
public class PolicyChecker
{
    private final List<Policy> list;
    private static final Logger LOG = LoggerFactory.getLogger(PolicyChecker.class);
    
    public PolicyChecker(List<Policy> list) {
        this.list = list;
    }
        
    public PolicyCheckResults executePolicies() {
        PolicyCheckResults results = new PolicyCheckResults();
        for (Policy p : this.list) {
            Policy.Result result = p.executePolicy();
            results.getPolicyResults().put(result, p.getType());
            LOG.info("Policy " + p + " of type " + p.getType() + " executed with result " + result);
        }
        return results;
    }
}