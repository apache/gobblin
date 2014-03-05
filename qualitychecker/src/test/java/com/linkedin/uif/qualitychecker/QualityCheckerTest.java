package com.linkedin.uif.qualitychecker;

import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.State;

public class QualityCheckerTest
{
    
    @Test
    public void testPolicyChecker() throws Exception {        
        State state = new State();
        state.setProp(ConfigurationKeys.QUALITY_CHECKER_PREFIX + ConfigurationKeys.POLICY_LIST, "com.linkedin.uif.qualitychecker.TestPolicy");
        state.setProp(ConfigurationKeys.QUALITY_CHECKER_PREFIX + ConfigurationKeys.POLICY_LIST_TYPE, "MANDATORY");

        PolicyCheckResults results = getPolicyResults(state);
        for (Map.Entry<Policy.Result, Policy.Type> entry : results.getPolicyResults().entrySet()) {
            Assert.assertEquals(entry.getKey(), Policy.Result.PASSED);
        }
    }
    
    public void testMultiplePolicies() throws Exception {        
        State state = new State();
        state.setProp(ConfigurationKeys.QUALITY_CHECKER_PREFIX + ConfigurationKeys.POLICY_LIST, "com.linkedin.uif.qualitychecker.TestPolicy,com.linkedin.uif.qualitychecker.TestPolicy");
        state.setProp(ConfigurationKeys.QUALITY_CHECKER_PREFIX + ConfigurationKeys.POLICY_LIST_TYPE, "MANDATORY,MANDATORY");

        PolicyCheckResults results = getPolicyResults(state);
        for (Map.Entry<Policy.Result, Policy.Type> entry : results.getPolicyResults().entrySet()) {
            Assert.assertEquals(entry.getKey(), Policy.Result.PASSED);
        }
    }
    
    public PolicyCheckResults getPolicyResults(State state) throws Exception {
        PolicyChecker checker = new PolicyCheckerBuilderFactory().newPolicyCheckerBuilder(state).build();
        return checker.executePolicies();
    }
}
