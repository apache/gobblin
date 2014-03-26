package com.linkedin.uif.configuration;

import java.lang.reflect.Constructor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetaStoreClientBuilder
{
    private State state;
    
    private static final Logger LOG = LoggerFactory.getLogger(MetaStoreClientBuilder.class);
    
    public MetaStoreClientBuilder(State state) {
        this.state = state;
    }
    
    public static MetaStoreClientBuilder newBuilder(State state) {
        return new MetaStoreClientBuilder(state);
    }
    
    @SuppressWarnings("unchecked")
    private MetaStoreClient createMetadataCollector() throws Exception {
        MetaStoreClient metaStoreClient;
        String metaStoreClientString = this.state.getProp(ConfigurationKeys.METADATA_CLIENT);
        try {
            Class<? extends MetaStoreClient> metaStoreClientClass = (Class<? extends MetaStoreClient>) Class.forName(metaStoreClientString);
            Constructor<? extends MetaStoreClient> metaStoreClientConstructor = metaStoreClientClass.getConstructor();
            metaStoreClient = metaStoreClientConstructor.newInstance();
        } catch (Exception e) {
            LOG.error(ConfigurationKeys.METADATA_CLIENT + " contains a class " + metaStoreClientString + " which doesn't extend MetaStoreClient");
            throw e;
        }
        return metaStoreClient;
    }
    
    public MetaStoreClient build() throws Exception {
        return createMetadataCollector();
    }
}
