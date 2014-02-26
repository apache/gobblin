package com.linkedin.uif.configuration;

import java.io.IOException;

public class YushanMetaStoreClient implements MetaStoreClient
{

    @Override
    public boolean initialize() throws IOException
    {
        return false;
    }

    @Override
    public State getMetadata() throws Exception
    {
        return null;
    }

    @Override
    public boolean sendMetadata(State state) throws Exception
    {
        return false;
    }

    @Override
    public boolean close() throws Exception
    {
        return false;
    }
}
