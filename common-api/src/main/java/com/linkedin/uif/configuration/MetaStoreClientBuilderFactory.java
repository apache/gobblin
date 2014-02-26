package com.linkedin.uif.configuration;

public class MetaStoreClientBuilderFactory
{
    public MetaStoreClientBuilder newMetaStoreClientBuilder(State state) {
        return new MetaStoreClientBuilder(state);
    }
}
