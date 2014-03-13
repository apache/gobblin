package com.linkedin.uif.publisher;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.State;
import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.source.workunit.Extract;

public class BaseDataPublisher extends DataPublisher
{
    private FileSystem fs;
    private final Map<Extract, List<WorkUnitState>> extractToStateMap;
    
    private static final Log LOG = LogFactory.getLog(BaseDataPublisher.class);
        
    public BaseDataPublisher(State state)
    {
        super(state);
        extractToStateMap = new HashMap<Extract, List<WorkUnitState>>();
    }

    @Override
    public void initialize() throws Exception {
        this.fs = FileSystem.get(new URI(getState().getProp(ConfigurationKeys.WRITER_FILE_SYSTEM_URI)), new Configuration());
    }
    
    @Override
    public void close() throws Exception {
        this.fs.close();
    }
    
    @Override
    public boolean publishData(Collection<? extends WorkUnitState> states) throws Exception {
        
        collectExtractMapping(states);

        for (Map.Entry<Extract, List<WorkUnitState>> entry : extractToStateMap.entrySet()) {
            Extract extract = entry.getKey();
            WorkUnitState workUnitState = entry.getValue().get(0);

            Path tmpOutput = new Path(workUnitState.getProp(ConfigurationKeys.WRITER_OUTPUT_DIR),
                                      extract.getNamespace().replaceAll("\\.", "/") + "/" + 
                                      extract.getTable() + "/" + extract.getExtractId() + "_" + 
                                      (extract.getIsFull() ? "FULL" : "APPEND"));
            
            Path finalOutput = new Path(workUnitState.getProp(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR),
                                      extract.getNamespace().replaceAll("\\.", "/") + "/" + 
                                      extract.getTable() + "/" + extract.getExtractId() + "_" + 
                                      (extract.getIsFull() ? "FULL" : "APPEND"));

            LOG.info(String.format("Attemping to move %s to %s", tmpOutput, finalOutput));

            if (this.fs.exists(finalOutput)) {
                if (this.getState().getPropAsBoolean((ConfigurationKeys.DATA_PUBLISHER_REPLACE_FINAL_DIR))) {
                    this.fs.delete(finalOutput, true);
                } else {
                    throw new IOException("Failed to publish data, final output path already exists");
                }
            }
            
            if (this.fs.rename(tmpOutput, finalOutput)) {
                for (WorkUnitState state : entry.getValue()) {
                    state.setWorkingState(WorkUnitState.WorkingState.COMMITTED);
                }
            } else {
                return false;
            }
        }
        return true;
    }
    
    private void collectExtractMapping(Collection<? extends WorkUnitState> states) {
        for (WorkUnitState state : states) {
            if (!state.getWorkingState().equals(WorkUnitState.WorkingState.COMMITTED)) {
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
    public boolean publishMetadata(Collection<? extends WorkUnitState> states) throws Exception {
        return true;
    }
}
