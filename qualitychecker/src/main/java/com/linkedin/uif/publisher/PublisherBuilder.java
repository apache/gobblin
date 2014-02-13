package com.linkedin.uif.publisher;

import com.linkedin.uif.metadata.MetadataCollector;
import com.linkedin.uif.qualitychecker.PolicyCheckResults;

public class PublisherBuilder
{
    private MetadataCollector metadata;
    private PolicyCheckResults results;
    
    public PublisherBuilder(PolicyCheckResults results, MetadataCollector metadata) {
        this.metadata = metadata;
        this.results = results;
    }
    
    public static PublisherBuilder newBuilder(PolicyCheckResults results, MetadataCollector metadata) {
        return new PublisherBuilder(results, metadata);
    }
    
    public Publisher build() {
        return new Publisher(this.results, this.metadata);
    }
}
