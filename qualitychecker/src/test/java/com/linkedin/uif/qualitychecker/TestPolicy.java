package com.linkedin.uif.qualitychecker;

import com.linkedin.uif.configuration.MetaStoreClient;
import com.linkedin.uif.configuration.State;

public class TestPolicy extends Policy
{
    public TestPolicy(State state, MetaStoreClient metadata, Type type)
    {
        super(state, metadata, type);
    }

    @Override
    public QualityCheckResult executePolicy()
    {
        return QualityCheckResult.PASSED;
    }
}