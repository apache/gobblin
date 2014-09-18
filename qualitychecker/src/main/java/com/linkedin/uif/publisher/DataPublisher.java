package com.linkedin.uif.publisher;

import java.util.Collection;

import com.linkedin.uif.configuration.State;
import com.linkedin.uif.configuration.WorkUnitState;

/**
 * Defines how to publish data and its corresponding metadata
 * Can be used for either task level or job level publishing
 */
public abstract class DataPublisher
{
    protected State state;

    public DataPublisher(State state) {
        this.setState(state);
    }
    
    public abstract void initialize() throws Exception;
    
    public abstract void close() throws Exception;
    
    /**
     * Returns true if it successfully publishes the data,
     * false otherwise
     */
    public abstract void publishData(Collection<? extends WorkUnitState> tasks) throws Exception;
        
    /**
     * Returns true if it successfully publishes the metadata,
     * false otherwise. Examples are checkpoint files, offsets, etc.
     */
    public abstract void publishMetadata(Collection<? extends WorkUnitState> tasks) throws Exception;
    
    public void publish(Collection<? extends WorkUnitState> tasks) throws Exception {
        publishMetadata(tasks);
        publishData(tasks);
    }
    
    public State getState()
    {
        return state;
    }

    public void setState(State state)
    {
        this.state = state;
    }
}
