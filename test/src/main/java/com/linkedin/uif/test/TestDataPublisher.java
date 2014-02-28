package com.linkedin.uif.test;

import java.util.Collection;

import com.linkedin.uif.configuration.State;
import com.linkedin.uif.publisher.DataPublisher;

/**
 * An implementation of {@link DataPublisher} for integration test.
 *
 * <p>
 *     This is a dummy implementation that exists purely to make
 *     integration test work.
 * </p>
 */
public class TestDataPublisher extends DataPublisher {

    public TestDataPublisher(State state) {
        super(state);
    }

    @Override
    public void initialize() throws Exception {

    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public boolean publishData(State state) throws Exception {
        return true;
    }

    @Override
    public boolean publishMetadata(State state) throws Exception {
        return true;
    }
    
    @Override
    public boolean publishData(Collection<? extends State> states) throws Exception {
        return true;
    }

    @Override
    public boolean publishMetadata(Collection<? extends State> states) throws Exception {
        return true;
    }
}
