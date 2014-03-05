package com.linkedin.uif.publisher;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.qualitychecker.Policy;
import com.linkedin.uif.qualitychecker.PolicyCheckResults;

public class TaskPublisher
{
    private final PolicyCheckResults results;
    private final WorkUnitState workUnitState;

    private static final Log LOG = LogFactory.getLog(TaskPublisher.class);
    
    public enum PublisherState {
        SUCCESS,                 // Data and metadata are successfully published
        CLEANUP_FAIL,            // Data and metadata were published, but cleanup failed
        POLICY_TESTS_FAIL,       // All tests didn't pass, no data committed
        COMPONENTS_NOT_FINISHED  // All components did not complete, no data committed
    };
    
    public TaskPublisher(WorkUnitState workUnitState, PolicyCheckResults results) throws Exception {

        this.results = results;
        this.workUnitState = workUnitState;
    }
    
    public PublisherState canPublish() throws Exception {
        if (allComponentsFinished()) {
            LOG.info("All components finished successfully, checking quality tests");
            if (passedAllTests()) {
                LOG.info("All required test passed, committing data");
                if (cleanup()) {
                    LOG.info("Cleanup executed successfully, finishing task");
                    return PublisherState.SUCCESS;
                } else {
                    return PublisherState.CLEANUP_FAIL;
                }
            } else {
                return PublisherState.POLICY_TESTS_FAIL;
            }
        } else {
            return PublisherState.COMPONENTS_NOT_FINISHED;
        }
    }
    
    /**
     * Returns true if all tests from the PolicyChecker pass, false otherwise
     */
    public boolean passedAllTests() {
        for ( Map.Entry<Policy.Result, Policy.Type> entry : results.getPolicyResults().entrySet()) {
            if (entry.getKey().equals(Policy.Result.FAILED) && entry.getValue().equals(Policy.Type.MANDATORY)) {
                return false;
            }
        }
        return true;
    }
    
    /**
     * Returns true if all the components finished, false otherwise
     */
    public boolean allComponentsFinished() {
        // Have to parse some information from TaskState
        return true;
    }
    
    /**
     * Cleans up any tmp folders used by the Task
     * Return true if successful, false otherwise
     */
    public boolean cleanup() throws Exception {
        return true;
    }
}
