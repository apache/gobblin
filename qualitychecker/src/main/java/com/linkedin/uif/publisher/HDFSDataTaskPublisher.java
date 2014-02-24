package com.linkedin.uif.publisher;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.scheduler.TaskState;

public class HDFSDataTaskPublisher extends DataPublisher
{
    private FileSystem fs;
    private Path stagingDataFile;
    private Path outputDataFile;
    private Path stagingMetadataFile;
    private Path outputMetadataFile;
    
    public HDFSDataTaskPublisher(TaskState taskState)
    {
        super(taskState);
    }

    @Override
    public void initialize() throws Exception {
        TaskState state = (TaskState) getState();
        this.fs = FileSystem.get(new URI(state.getProp(ConfigurationKeys.FILE_SYSTEM_URI_KEY)), new Configuration());
        this.stagingDataFile = new Path(state.getProp(ConfigurationKeys.TASK_PUBLISHER_PREFIX + ConfigurationKeys.HDFS_TASK_DATA_TMP_DIR) + state.getTaskId());
        this.outputDataFile = new Path(state.getProp(ConfigurationKeys.TASK_PUBLISHER_PREFIX + ConfigurationKeys.HDFS_TASK_DATA_FINAL_DIR) + state.getTaskId());
        this.stagingMetadataFile = new Path(state.getProp(ConfigurationKeys.TASK_PUBLISHER_PREFIX + ConfigurationKeys.HDFS_TASK_METADATA_TMP_DIR) + state.getTaskId());
        this.outputMetadataFile = new Path(state.getProp(ConfigurationKeys.TASK_PUBLISHER_PREFIX + ConfigurationKeys.HDFS_TASK_METADATA_FINAL_DIR) + state.getTaskId());
    }
    
    @Override
    public void close() throws Exception {
        fs.close();
    }
    
    @Override
    public boolean publishData() throws IOException {
        if (this.fs.exists(this.outputDataFile)) {
            throw new IOException("File " + this.outputDataFile + " already exists");
        }
        return this.fs.rename(this.stagingDataFile, this.outputDataFile);
    }

    @Override
    public boolean publishMetadata() throws IOException {
        if (this.fs.exists(this.outputMetadataFile)) {
            throw new IOException("File " + this.outputMetadataFile + " already exists");
        }
        return this.fs.rename(this.stagingMetadataFile, this.outputMetadataFile);
    }
}
