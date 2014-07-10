package com.linkedin.uif.source.extractor.extract;

import java.io.IOException;
import java.util.List;

import com.linkedin.uif.configuration.SourceState;
import com.linkedin.uif.configuration.State;
import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.source.extractor.Extractor;

public class FileSystemSource<K, V> extends FileBasedSource<K, V>
{

    @Override
    public Extractor<K, V> getExtractor(WorkUnitState state) throws IOException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void shutdown(SourceState state)
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public List<String> getcurrentFsSnapshot(State state)
    {
        // TODO Auto-generated method stub
        return null;
    }

}
