package com.linkedin.uif.publisher;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.State;
import com.linkedin.uif.configuration.WorkUnitState;

public class BaseDataPublisher extends DataPublisher
{
    private FileSystem fs;
        
    public BaseDataPublisher(State state)
    {
        super(state);
    }

    @Override
    public void initialize() throws Exception {
        this.fs = FileSystem.get(new URI(getState().getProp(ConfigurationKeys.FILE_SYSTEM_URI_KEY)), new Configuration());
    }
    
    @Override
    public void close() throws Exception {
        fs.close();
    }
    
    @Override
    public boolean publishData(State state) throws IOException {       
        WorkUnitState task = (WorkUnitState) state;
        Path stagingDataDir = new Path(task.getProp(ConfigurationKeys.OUTPUT_DIR_KEY));
        Path outputDataDir = new Path(task.getProp(ConfigurationKeys.JOB_FINAL_DIR_HDFS));

        if (!this.fs.exists(outputDataDir)) {
            fs.mkdirs(outputDataDir);
        }
        
        for (FileStatus status : fs.listStatus(stagingDataDir)) {
            if (!this.fs.rename(status.getPath(), new Path(outputDataDir, status.getPath().getName()))) return false;
        }
        return true;
    }

    @Override
    public boolean publishMetadata(State state) throws IOException {
        return true;
    }

    @Override
    public boolean publishData(Collection<? extends State> states) throws Exception
    {
        for (State state : states) {
            if ( !this.publishData(state) ) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean publishMetadata(Collection<? extends State> states) throws Exception
    {
        for (State state : states) {
            if ( !this.publishMetadata(state) ) {
                return false;
            }
        }
        return true;
    }
}
