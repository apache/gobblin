package com.linkedin.uif.configuration;


/**
 * Defines an interface for interacting with an external metastore (Yushan, HDFS, etc.)
 * Useful to pull in extra information for uses cases including cross ETL flow validation
 * or historical trend analysis
 */
public interface MetaStoreClient
{
    /**
     * Does any initialization e.g. establishing a connection to a metastore
     */
    public boolean initialize() throws Exception;
    
    /**
     * Returns a properties file containing key, value pair statistics
     * about the data stored
     */
    public State getMetadata() throws Exception;
    
    /**
     * Sends metadata to a metastore
     */
    public boolean sendMetadata(State state) throws Exception;
    
    /**
     * Closes any state the class uses e.g. an opened connection
     */
    public boolean close() throws Exception;
}
