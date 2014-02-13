package com.linkedin.uif.metadata;

import java.util.Properties;

import com.linkedin.uif.configuration.WorkUnitState;

public class WorkUnitMetadataCollector implements MetadataCollector
{
    private WorkUnitState state;
    
    public WorkUnitMetadataCollector(WorkUnitState state) {
        this.state = state;
    }
    
    @Override
    public Properties getDataProperties()
    {
        // return this.state.getProperties();
        return null;
    }

    @Override
    public boolean setDataMetadata()
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean sendDataMetadata(Properties props)
    {
        // this.state.addAll(props);
        return false;
    }

    @Override
    public Properties getJobProperties()
    {
        // return this.state.getProperties();
        return null;
    }

    @Override
    public boolean setJobMetadata()
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean sendJobMetadata(Properties props)
    {
        // this.state.addAll(props);
        return false;
    }
}
