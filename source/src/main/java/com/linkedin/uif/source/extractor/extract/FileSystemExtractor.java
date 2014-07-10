package com.linkedin.uif.source.extractor.extract;

import java.io.IOException;
import java.net.URI;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.WorkUnitState;

public abstract class FileSystemExtractor<S, D, K extends Command, V> extends FileBasedExtractor<S, D, K, V>
{
    protected FileSystem fs;
    
    private Logger log = LoggerFactory.getLogger(FileSystemExtractor.class);
    
    public FileSystemExtractor(WorkUnitState workUnitState)
    {
        super(workUnitState);
        this.fs = createFileSystem(workUnitState.getProp(ConfigurationKeys.SOURCE_FILEBASED_FS_URI));
    }
    
    private FileSystem createFileSystem(String uri) {
        try {
            return FileSystem.get(new URI(workUnitState.getProp(ConfigurationKeys.SOURCE_FILEBASED_FS_URI)), new Configuration());
        } catch (Exception e) {
            Throwables.propagate(e);
            return null;
        }
    }

    @Override
    public void close()
    {
        try {
            this.fs.close();
        } catch (IOException e) {
            log.error("Unable to close filesystem: " + e.getMessage(), e);
        }
    }

    @Override
    public CommandOutput<K, V> executeCommands(List<Command> cmds)
    {
        // TODO Auto-generated method stub
        return null;
    }
}
