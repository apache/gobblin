package com.linkedin.uif.publisher;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.State;
import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.source.workunit.Extract;

public class BaseDataPublisher extends DataPublisher {

    protected FileSystem fs;
    protected final Map<Extract, List<WorkUnitState>> extractToStateMap;

    private static final Logger LOG = LoggerFactory.getLogger(BaseDataPublisher.class);

    public BaseDataPublisher(State state) {
        super(state);
        extractToStateMap = new HashMap<Extract, List<WorkUnitState>>();
    }

    @Override
    public void initialize() throws IOException {
        Configuration conf = new Configuration();
        // Add all job configuration properties so they are picked up by Hadoop
        for (String key : this.state.getPropertyNames()) {
            conf.set(key, this.state.getProp(key));
        }
        this.fs = FileSystem.get(URI.create(getState().getProp(ConfigurationKeys.WRITER_FILE_SYSTEM_URI)), conf);
    }

    @Override
    public void close() throws IOException {
        // Nothing to do
    }

    @Override
    public void publishData(Collection<? extends WorkUnitState> states) throws IOException {

        collectExtractMapping(states);

        for (Map.Entry<Extract, List<WorkUnitState>> entry : extractToStateMap.entrySet()) {
            Extract extract = entry.getKey();
            WorkUnitState workUnitState = entry.getValue().get(0);

            Path tmpOutput = new Path(workUnitState.getProp(ConfigurationKeys.WRITER_OUTPUT_DIR),
                                      extract.getOutputFilePath());

            Path finalOutput = new Path(workUnitState.getProp(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR),
                                      extract.getOutputFilePath());

            // Create the parent directory of the final output directory if it does not exist
            if (!this.fs.exists(finalOutput.getParent())) {
                this.fs.mkdirs(finalOutput.getParent());
            }

            LOG.info(String.format("Attempting to move %s to %s", tmpOutput, finalOutput));

            if (this.fs.exists(finalOutput)) {
                if (this.getState().getPropAsBoolean((ConfigurationKeys.DATA_PUBLISHER_REPLACE_FINAL_DIR))) {
                    this.fs.delete(finalOutput, true);
                } else {
                    // Add the files to the existing output folder
                    // TODO this code has not been tested
                    // TODO this does not publish the data atomically
                    for (FileStatus status : this.fs.listStatus(tmpOutput)) {
                        if (workUnitState.getPropAsBoolean(ConfigurationKeys.SOURCE_FILEBASED_PRESERVE_FILE_PATH, false)) {
                            this.fs.rename(status.getPath(), new Path(finalOutput, workUnitState.getProp(ConfigurationKeys.DATA_PUBLISHER_FINAL_NAME)));
                        } else {
                            this.fs.rename(status.getPath(), new Path(finalOutput, status.getPath().getName()));
                        }
                    }
                    continue;
                }
            }

            if (this.fs.rename(tmpOutput, finalOutput)) {
                // Upon successfully committing the data to the final
                // output directory, set states of all tasks to COMMITTED.
                for (WorkUnitState state : entry.getValue()) {
                    state.setWorkingState(WorkUnitState.WorkingState.COMMITTED);
                }
            } else {
                throw new RuntimeException("Rename operation from " + tmpOutput + " to " + finalOutput + " failed!");
            }
        }
    }

    protected void collectExtractMapping(Collection<? extends WorkUnitState> states) {
        for (WorkUnitState state : states) {
            if (!state.getWorkingState().equals(WorkUnitState.WorkingState.SUCCESSFUL)) {
                continue;
            }

            if (!extractToStateMap.containsKey(state.getExtract())) {
                List<WorkUnitState> list = new ArrayList<WorkUnitState>();
                list.add(state);
                extractToStateMap.put(state.getExtract(), list);
            } else {
                extractToStateMap.get(state.getExtract()).add(state);
            }
        }
    }

    @Override
    public void publishMetadata(Collection<? extends WorkUnitState> states) throws IOException {
    }
}
