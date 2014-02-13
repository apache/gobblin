package com.linkedin.uif.metadata;

import java.util.Properties;

/**
 * Defines an interface for classes which will feed metadata to the PolicyChecker and Publisher
 */
public interface MetadataCollector
{
    /**
     * Returns a properties file containing key, value pair statistics
     * about the data stored
     */
    public Properties getDataProperties();
    
    /**
     * Attempts to set the data metadata from some source
     * Returns true if successful, false otherwise
     */
    public boolean setDataMetadata();
    
    /**
     * Sends data metadata to a metastore
     */
    public boolean sendDataMetadata(Properties props);
    
    /**
     * Returns a properties file containing key, value pair statistics
     * about the job
     */
    public Properties getJobProperties();
    
    /**
     * Attempts to set the job metadata from some source
     * Returns true if successful, false otherwise
     */
    public boolean setJobMetadata();
    
    /**
     * Sends job metadata to a metastore
     */
    public boolean sendJobMetadata(Properties props);
}
