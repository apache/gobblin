package com.linkedin.uif.test;

import java.util.Collection;

import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.publisher.DataPublisher;
import com.linkedin.uif.scheduler.JobState;

/**
 * An implementation of {@link DataPublisher} for integration test.
 *
 * <p>
 *     This is a dummy implementation that exists purely to make
 *     integration test work.
 * </p>
 */
public class TestDataPublisher extends DataPublisher {

    public TestDataPublisher(JobState state) {
        super(state);
    }

    @Override
    public void initialize() throws Exception
    {
        // Do nothing
    }

    @Override
    public void close() throws Exception
    {
        // Do nothing
    }

    @Override
    public boolean publishData() throws Exception
    {
        return true;
    }

    @Override
    public boolean collectTaskData(Collection<? extends WorkUnitState> tasks) throws Exception
    {
        return true;
    }

    @Override
    public boolean publishMetadata() throws Exception
    {
        return true;
    }

    @Override
    public boolean collectTaskMetadata(Collection<? extends WorkUnitState> tasks) throws Exception
    {
        return true;
    }
}
