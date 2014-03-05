package com.linkedin.uif.qualitychecker;

import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.State;

@Test(groups = {"com.linkedin.uif.qualitychecker"})
public class RowCountPolicyTest
{
    
    public static final long EXTRACTOR_ROWS_READ = 1000;
    public static final long WRITER_ROWS_WRITTEN = 1000;
    
    @Test
    public void testRowCountPolicyPassed() throws Exception {        
        State state = new State();
        state.setProp(ConfigurationKeys.QUALITY_CHECKER_PREFIX + ConfigurationKeys.POLICY_LIST, "com.linkedin.uif.qualitychecker.RowCountPolicy");
        state.setProp(ConfigurationKeys.QUALITY_CHECKER_PREFIX + ConfigurationKeys.POLICY_LIST_TYPE, "MANDATORY");
        state.setProp(ConfigurationKeys.QUALITY_CHECKER_PREFIX + ConfigurationKeys.EXTRACTOR_ROWS_READ, EXTRACTOR_ROWS_READ);
        state.setProp(ConfigurationKeys.QUALITY_CHECKER_PREFIX + ConfigurationKeys.WRITER_ROWS_WRITTEN, WRITER_ROWS_WRITTEN);

        PolicyCheckResults results = getPolicyResults(state);
        for (Map.Entry<Policy.Result, Policy.Type> entry : results.getPolicyResults().entrySet()) {
            Assert.assertEquals(entry.getKey(), Policy.Result.PASSED);
        }
    }
    
    @Test
    public void testRowCountPolicyFailed() throws Exception {        
        State state = new State();
        state.setProp(ConfigurationKeys.QUALITY_CHECKER_PREFIX + ConfigurationKeys.POLICY_LIST, "com.linkedin.uif.qualitychecker.RowCountPolicy");
        state.setProp(ConfigurationKeys.QUALITY_CHECKER_PREFIX + ConfigurationKeys.POLICY_LIST_TYPE, "MANDATORY");
        state.setProp(ConfigurationKeys.QUALITY_CHECKER_PREFIX + ConfigurationKeys.EXTRACTOR_ROWS_READ, EXTRACTOR_ROWS_READ);
        state.setProp(ConfigurationKeys.QUALITY_CHECKER_PREFIX + ConfigurationKeys.WRITER_ROWS_WRITTEN, -1);

        PolicyCheckResults results = getPolicyResults(state);
        for (Map.Entry<Policy.Result, Policy.Type> entry : results.getPolicyResults().entrySet()) {
            Assert.assertEquals(entry.getKey(), Policy.Result.FAILED);
        }
    }
    
    @Test
    public void testRowCountRangePolicyPassedExact() throws Exception {        
        State state = new State();
        state.setProp(ConfigurationKeys.QUALITY_CHECKER_PREFIX + ConfigurationKeys.POLICY_LIST, "com.linkedin.uif.qualitychecker.RowCountRangePolicy");
        state.setProp(ConfigurationKeys.QUALITY_CHECKER_PREFIX + ConfigurationKeys.POLICY_LIST_TYPE, "MANDATORY");
        state.setProp(ConfigurationKeys.QUALITY_CHECKER_PREFIX + ConfigurationKeys.EXTRACTOR_ROWS_READ, EXTRACTOR_ROWS_READ);
        state.setProp(ConfigurationKeys.QUALITY_CHECKER_PREFIX + ConfigurationKeys.WRITER_ROWS_WRITTEN, WRITER_ROWS_WRITTEN);
        state.setProp(ConfigurationKeys.QUALITY_CHECKER_PREFIX + ConfigurationKeys.ROW_COUNT_RANGE, "0.05");

        PolicyCheckResults results = getPolicyResults(state);
        for (Map.Entry<Policy.Result, Policy.Type> entry : results.getPolicyResults().entrySet()) {
            Assert.assertEquals(entry.getKey(), Policy.Result.PASSED);
        }
    }
    
    @Test
    public void testRowCountRangePolicyPassedRange() throws Exception {        
        State state = new State();
        state.setProp(ConfigurationKeys.QUALITY_CHECKER_PREFIX + ConfigurationKeys.POLICY_LIST, "com.linkedin.uif.qualitychecker.RowCountRangePolicy");
        state.setProp(ConfigurationKeys.QUALITY_CHECKER_PREFIX + ConfigurationKeys.POLICY_LIST_TYPE, "MANDATORY");
        state.setProp(ConfigurationKeys.QUALITY_CHECKER_PREFIX + ConfigurationKeys.EXTRACTOR_ROWS_READ, EXTRACTOR_ROWS_READ);
        state.setProp(ConfigurationKeys.QUALITY_CHECKER_PREFIX + ConfigurationKeys.WRITER_ROWS_WRITTEN, (long) 0.03*EXTRACTOR_ROWS_READ + EXTRACTOR_ROWS_READ);
        state.setProp(ConfigurationKeys.QUALITY_CHECKER_PREFIX + ConfigurationKeys.ROW_COUNT_RANGE, "0.05");

        PolicyCheckResults results = getPolicyResults(state);
        for (Map.Entry<Policy.Result, Policy.Type> entry : results.getPolicyResults().entrySet()) {
            Assert.assertEquals(entry.getKey(), Policy.Result.PASSED);
        }
    }
    
    @Test
    public void testRowCountRangePolicyFailed() throws Exception {        
        State state = new State();
        state.setProp(ConfigurationKeys.QUALITY_CHECKER_PREFIX + ConfigurationKeys.POLICY_LIST, "com.linkedin.uif.qualitychecker.RowCountRangePolicy");
        state.setProp(ConfigurationKeys.QUALITY_CHECKER_PREFIX + ConfigurationKeys.POLICY_LIST_TYPE, "MANDATORY");
        state.setProp(ConfigurationKeys.QUALITY_CHECKER_PREFIX + ConfigurationKeys.EXTRACTOR_ROWS_READ, EXTRACTOR_ROWS_READ);
        state.setProp(ConfigurationKeys.QUALITY_CHECKER_PREFIX + ConfigurationKeys.WRITER_ROWS_WRITTEN, -1);
        state.setProp(ConfigurationKeys.QUALITY_CHECKER_PREFIX + ConfigurationKeys.ROW_COUNT_RANGE, "0.05");

        PolicyCheckResults results = getPolicyResults(state);
        for (Map.Entry<Policy.Result, Policy.Type> entry : results.getPolicyResults().entrySet()) {
            Assert.assertEquals(entry.getKey(), Policy.Result.FAILED);
        }
    }
    
    @Test
    public void testMultipleRowCountPolicies() throws Exception {        
        State state = new State();
        state.setProp(ConfigurationKeys.QUALITY_CHECKER_PREFIX + ConfigurationKeys.POLICY_LIST, "com.linkedin.uif.qualitychecker.RowCountPolicy,com.linkedin.uif.qualitychecker.RowCountRangePolicy");
        state.setProp(ConfigurationKeys.QUALITY_CHECKER_PREFIX + ConfigurationKeys.POLICY_LIST_TYPE, "MANDATORY,MANDATORY");
        state.setProp(ConfigurationKeys.QUALITY_CHECKER_PREFIX + ConfigurationKeys.EXTRACTOR_ROWS_READ, EXTRACTOR_ROWS_READ);
        state.setProp(ConfigurationKeys.QUALITY_CHECKER_PREFIX + ConfigurationKeys.WRITER_ROWS_WRITTEN, WRITER_ROWS_WRITTEN);
        state.setProp(ConfigurationKeys.QUALITY_CHECKER_PREFIX + ConfigurationKeys.ROW_COUNT_RANGE, "0.05");

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
