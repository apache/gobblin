package com.linkedin.uif.qualitychecker;

import java.net.URI;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.State;
import com.linkedin.uif.qualitychecker.task.TaskLevelPolicy;
import com.linkedin.uif.qualitychecker.task.TaskLevelPolicyCheckResults;
import com.linkedin.uif.qualitychecker.task.TaskLevelPolicyChecker;
import com.linkedin.uif.qualitychecker.task.TaskLevelPolicyCheckerBuilderFactory;

@Test(groups = {"com.linkedin.uif.qualitychecker"})
public class TaskLevelQualityCheckerTest
{
    
    @Test
    public void testPolicyChecker() throws Exception {        
        State state = new State();
        state.setProp(ConfigurationKeys.TASK_LEVEL_POLICY_LIST, "com.linkedin.uif.qualitychecker.TestTaskLevelPolicy");
        state.setProp(ConfigurationKeys.TASK_LEVEL_POLICY_LIST_TYPE, "FAIL");

        TaskLevelPolicyCheckResults results = getPolicyResults(state);
        for (Map.Entry<TaskLevelPolicy.Result, TaskLevelPolicy.Type> entry : results.getPolicyResults().entrySet()) {
            Assert.assertEquals(entry.getKey(), TaskLevelPolicy.Result.PASSED);
        }
    }
    
    @Test
    public void testMultiplePolicies() throws Exception {        
        State state = new State();
        state.setProp(ConfigurationKeys.TASK_LEVEL_POLICY_LIST, "com.linkedin.uif.qualitychecker.TestTaskLevelPolicy,com.linkedin.uif.qualitychecker.TestTaskLevelPolicy");
        state.setProp(ConfigurationKeys.TASK_LEVEL_POLICY_LIST_TYPE, "FAIL,FAIL");

        TaskLevelPolicyCheckResults results = getPolicyResults(state);
        for (Map.Entry<TaskLevelPolicy.Result, TaskLevelPolicy.Type> entry : results.getPolicyResults().entrySet()) {
            Assert.assertEquals(entry.getKey(), TaskLevelPolicy.Result.PASSED);
        }
    }
    
    private TaskLevelPolicyCheckResults getPolicyResults(State state) throws Exception {
        TaskLevelPolicyChecker checker = new TaskLevelPolicyCheckerBuilderFactory().newPolicyCheckerBuilder(state, -1).build();
        return checker.executePolicies();
    }
}
