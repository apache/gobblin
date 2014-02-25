package com.linkedin.uif.publisher;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.linkedin.uif.configuration.MetaStoreClient;
import com.linkedin.uif.configuration.State;
import com.linkedin.uif.qualitychecker.Policy;
import com.linkedin.uif.qualitychecker.QualityCheckResult;
import com.linkedin.uif.qualitychecker.PolicyCheckResults;
import com.linkedin.uif.scheduler.TaskState;

public class TaskPublisher
{
    private final PolicyCheckResults results;
    private final DataPublisher dataPublisher;
    private final MetaStoreClient metadata;
    private final TaskState taskState;

    private static final Log LOG = LogFactory.getLog(TaskPublisher.class);
    
    public enum PublisherState {
        SUCCESS,                 // Data and metadata are successfully published
        CLEANUP_FAIL,            // Data and metadata were published, but cleanup failed
        METADATA_PUBLISH_FAIL,   // Data was published, but metadata wasn't
        DATA_PUBLISH_FAIL,       // Data failed to published, metadata was no published
        POLICY_TESTS_FAIL,       // All tests didn't pass, no data committed
        COMPONENTS_NOT_FINISHED  // All components did not complete, no data committed
    };
    
    public TaskPublisher(TaskState taskState, PolicyCheckResults results, MetaStoreClient metadata, DataPublisher dataPublisher) {
        this.results = results;
        this.metadata = metadata;
        this.dataPublisher = dataPublisher;
        this.taskState = taskState;
    }
    
    public PublisherState publish() throws Exception {
        if (allComponentsFinished()) {
            LOG.info("All components finished successfully, checking quality tests");
            if (passedAllTests()) {
                LOG.info("All required test passed, committing data");
                if (publishData()) {
                 LOG.info("Data committed successfully, updating metadata");
                    if (publishMetadata()) {
                        LOG.info("Metadata published successfully, cleaning up task");
                        if (cleanup()) {
                            LOG.info("Cleanup executed successfully, finishing task");
                            return PublisherState.SUCCESS;
                        } else {
                            LOG.error("Cleanup failed, exiting task");
                            return PublisherState.CLEANUP_FAIL;
                        }
                    } else {
                        LOG.error("Publishing metadata failed, exiting task");
                        return PublisherState.METADATA_PUBLISH_FAIL;
                    }
                } else {
                    LOG.error("Publishing data failed, exiting task");
                    return PublisherState.DATA_PUBLISH_FAIL;
                }
            } else {
                LOG.error("All tests were not passed, exiting task");
                return PublisherState.POLICY_TESTS_FAIL;
            }
        } else {
            LOG.error("All components did not finish, exiting task");
            return PublisherState.COMPONENTS_NOT_FINISHED;
        }
    }
    
    /**
     * Returns true if all tests from the PolicyChecker pass, false otherwise
     */
    public boolean passedAllTests() {
        for ( Map.Entry<QualityCheckResult, Policy.Type> entry : results.getPolicyResults().entrySet()) {
            if (entry.getKey().equals(QualityCheckResult.FAILED) && entry.getValue().equals(Policy.Type.MANDATORY)) {
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
        return false;
    }

    /**
     * Publishes the data and returns true if the data publish is successful, if not it returns false
     * @throws IOException 
     */
    public boolean publishData() throws Exception {
        return this.dataPublisher.publishData();
    }
    
    /**
     * Publishes the metadata (high water mark) to the metadata store (Yushan, HDFS, etc.)
     * Returns true if the publish is successful, false otherwise
     * @throws Exception 
     */
    public boolean publishMetadata() throws Exception {
        State state = new State();
        if (metadata.sendMetadata(state) && dataPublisher.publishMetadata()) {
            LOG.info("Metadata sent");
            return true;
        } else {
            LOG.error("Metadata was not sent successfully");
            return false;
        }
    }
    
    /**
     * Cleans up any tmp folders used by the Task
     * Return true if successful, false otherwise
     */
    public boolean cleanup() throws Exception {
        try {
            this.dataPublisher.close();
            return true;
        } catch (Exception e) {
            LOG.error(e);
            throw e;
        }
    }
}
