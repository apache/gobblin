/* (c) 2014 LinkedIn Corp. All rights reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

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

import com.google.common.collect.Lists;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.State;
import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.source.workunit.Extract;
import com.linkedin.uif.util.ForkOperatorUtils;

public class BaseDataPublisher extends DataPublisher {

    protected final List<FileSystem> fss = Lists.newArrayList();
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

        int branches = this.state.getPropAsInt(ConfigurationKeys.FORK_BRANCHES_KEY, 1);
        for (int i = 0; i < branches; i++) {
            URI uri = URI.create(getState().getProp(
                    ForkOperatorUtils.getPropertyNameForBranch(
                            ConfigurationKeys.WRITER_FILE_SYSTEM_URI, branches, i),
                    ConfigurationKeys.LOCAL_FS_URI));
            this.fss.add(FileSystem.get(uri, conf));
        }
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

            int branches = workUnitState.getPropAsInt(ConfigurationKeys.FORK_BRANCHES_KEY, 1);
            for (int i = 0; i < branches; i++) {
                String branchName = ForkOperatorUtils.getBranchName(
                        workUnitState, i, ConfigurationKeys.DEFAULT_FORK_BRANCH_NAME + i);

                Path tmpOutput = new Path(
                        workUnitState.getProp(ForkOperatorUtils.getPropertyNameForBranch(
                                ConfigurationKeys.WRITER_OUTPUT_DIR, branches, i)),
                        ForkOperatorUtils.getPathForBranch(extract.getOutputFilePath(), branchName, branches));

                Path finalOutput = new Path(
                        workUnitState.getProp(ForkOperatorUtils.getPropertyNameForBranch(
                                ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, branches, i)),
                        ForkOperatorUtils.getPathForBranch(extract.getOutputFilePath(), branchName, branches));

                // Create the parent directory of the final output directory if it does not exist
                if (!this.fss.get(i).exists(finalOutput.getParent())) {
                    this.fss.get(i).mkdirs(finalOutput.getParent());
                }

                LOG.info(String.format("Attempting to move %s to %s", tmpOutput, finalOutput));

                if (this.fss.get(i).exists(finalOutput)) {
                    if (this.getState().getPropAsBoolean(ForkOperatorUtils.getPropertyNameForBranch(
                            ConfigurationKeys.DATA_PUBLISHER_REPLACE_FINAL_DIR, branches, i))) {
                        this.fss.get(i).delete(finalOutput, true);
                    } else {
                        // Add the files to the existing output folder
                        // TODO this code has not been tested
                        // TODO this does not publish the data atomically
                        for (FileStatus status : this.fss.get(i).listStatus(tmpOutput)) {
                            if (workUnitState.getPropAsBoolean(ForkOperatorUtils.getPropertyNameForBranch(
                                            ConfigurationKeys.SOURCE_FILEBASED_PRESERVE_FILE_PATH, branches, i),
                                    false)) {
                                this.fss.get(i).rename(status.getPath(), new Path(finalOutput,
                                        workUnitState.getProp(ForkOperatorUtils.getPropertyNameForBranch(
                                                ConfigurationKeys.DATA_PUBLISHER_FINAL_NAME, branches, i))));
                            } else {
                                this.fss.get(i).rename(
                                        status.getPath(), new Path(finalOutput, status.getPath().getName()));
                            }
                        }
                        continue;
                    }
                }

                if (this.fss.get(i).rename(tmpOutput, finalOutput)) {
                    // Upon successfully committing the data to the final output directory, set states
                    // of successful tasks to COMMITTED. leaving states of unsuccessful ones unchanged.
                    // This makes sense to the COMMIT_ON_PARTIAL_SUCCESS policy.
                    for (WorkUnitState state : entry.getValue()) {
                        if (state.getWorkingState() == WorkUnitState.WorkingState.SUCCESSFUL) {
                            state.setWorkingState(WorkUnitState.WorkingState.COMMITTED);
                        }
                    }
                } else {
                    throw new RuntimeException(
                            "Rename operation from " + tmpOutput + " to " + finalOutput + " failed!");
                }
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
