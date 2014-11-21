/* (c) 2014 LinkedIn Corp. All rights reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.uif.configuration;

import java.io.IOException;

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
    public boolean initialize() throws IOException;
    
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
