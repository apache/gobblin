package com.linkedin.uif.schema;

import java.util.Date;
import java.util.Map;

import com.linkedin.uif.configuration.State;

/**
 * Interface to fetch schemas
 */
public interface SchemaRetriever
{
    /**
     * Initialize SchemaRetriever (e.g. initialize connection
     * to HDFS, Zookeeper, etc.)
     * @param state specifies configuration parameters
     * @return true if successful, false otherwise
     */
    public boolean initialize(State state);
    
    /**
     * Returns schema from previous execution of job
     * @return true if successful, false otherwise
     */
    public String getLatestPreviousSchema();
    
    /**
     * Returns schemas from all previous executions
     * @return Map of Date of the previous execution and
     * a string representing the schema
     */
    public Map<Date, String> getAllPreviousSchema();
    
    /**
     * Closes the schema retriever
     * @return true if successful, false otherwise
     */
    public boolean close();
}
