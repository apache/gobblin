package com.linkedin.uif.publisher;

import java.util.Properties;

import com.linkedin.uif.metadata.MetadataCollector;
import com.linkedin.uif.qualitychecker.PolicyResult;
import com.linkedin.uif.qualitychecker.PolicyCheckResults;

public class Publisher
{
    private PolicyCheckResults results;
    private MetadataCollector metadata;

    public enum PublisherState { SUCCESS, CLEANUP_FAIL, METADATA_FAIL, DATA_FAIL, TESTS_FAIL, COMPONENTS_FAIL } ;
    
    public Publisher(PolicyCheckResults results, MetadataCollector metadata) {
        this.results = results;
        this.metadata = metadata;
    }
    
    public PublisherState execute() {
        if (allComponentsFinished()) {
            // log.info("All components finished successfully, checking quality tests");
            if (passedAllTests()) {
                // log.info("All required test passed, committing data");
                if (publishData()) {
                 // log.info("Data committed successfully, updating metadata");
                    if (publishMetadata()) {
                        // log.info("Metadata published successfully, cleaning up task");
                        if (cleanup()) {
                            // log.info("Cleanup executed successfully, finishing task");
                            return PublisherState.SUCCESS;
                        } else {
                            // log.error("Cleanup failed, exiting task);
                            return PublisherState.CLEANUP_FAIL;
                        }
                    } else {
                        // log.error("Publishing metadata failed, exiting task);
                        return PublisherState.METADATA_FAIL;
                    }
                } else {
                    // log.error("Publishing data failed, exiting task);
                    return PublisherState.DATA_FAIL;
                }
            } else {
                // log.error("All tests were not passed, exiting task);
                return PublisherState.TESTS_FAIL;
            }
        } else {
            // log.error("All components did not finish, exiting task);
            return PublisherState.COMPONENTS_FAIL;
        }
    }
    
    /**
     * Returns true if all tests from the PolicyChecker pass, false otherwise
     */
    public boolean passedAllTests() {
        for ( PolicyResult result : results.getPolicyResults()) {
            if (result.getStatus().equals(PolicyResult.Status.FAILED)) {
                return false;
            }
        }
        return true;
    }
    
    /**
     * Returns true if all the components finished, false otherwise
     */
    public boolean allComponentsFinished() {
        return false;
    }

    /**
     * Publishes the data and returns true if the data publish is successful, if not it returns false
     */
    public boolean publishData() {
        return false;
    }
    
    /**
     * Publishes the metadata (high water mark) to the metadata store (Yushan, HDFS, etc.)
     * Returns true if the publish is successful, false otherwise
     */
    public boolean publishMetadata() {
        Properties props = new Properties();
        if (metadata.sendDataMetadata(props) && metadata.sendJobMetadata(props)) {
            // log.info("Metadata sent to metastore");
            return true;
        } else {
            // log.error("Metadata was not sent successfully");
            return false;
        }
    }
    
    /**
     * Cleans up any tmp folders used by the Task
     * Return true if successful, false otherwise
     */
    public boolean cleanup() {
        return false;
    }
}
