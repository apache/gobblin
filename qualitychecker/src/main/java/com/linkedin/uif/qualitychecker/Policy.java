package com.linkedin.uif.qualitychecker;

import com.linkedin.uif.metadata.MetadataCollector;

/**
 * Policy takes in a MetadataCollector objects which contains
 * stats taken from the extractor / writer or any stats taken
 * from some external source (e.g. HCat)
 */
public abstract class Policy
{   
    private MetadataCollector state;
    
    public Policy(MetadataCollector state) {
        this.state = state;
    }
    
    public MetadataCollector getState() {
        return this.state;
    }
    
    /**
     * Main method that defines the semantics of this policy
     * This method will be executed by the PolicyChecker
     */
    public abstract PolicyResult executePolicy();
}
