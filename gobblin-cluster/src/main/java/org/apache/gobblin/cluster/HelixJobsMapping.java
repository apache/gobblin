/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gobblin.cluster;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;

import javax.annotation.Nullable;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.metastore.FsStateStore;
import org.apache.gobblin.metastore.StateStore;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.util.ClassAliasResolver;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.JobLauncherUtils;


/**
 * <p> Any job that submitted to Helix will have a unique id.
 * We need some mapping between the job name and the job id,
 * in order to perform:
 *
 *  1) cancel a running job
 *  2) delete a running job
 *  3) block any incoming job with same name.
 *
 * <p> More complexity comes into the picture in the distributed
 * ask driver mode, where we will have a job name, which maps to a
 * planning job id and further maps to a real job id.
 *
 * <p> We will leverage the state store functionality. We will save
 * job name as a storeName, and tableName. The planning job id and
 * real job id will be saved in the state object.
 */
public class HelixJobsMapping {

  public static final String JOBS_MAPPING_DB_TABLE_KEY = "jobs.mapping.db.table.key";
  public static final String DEFAULT_JOBS_MAPPING_DB_TABLE_KEY_NAME = "JobsMapping";

  public static final String DISTRIBUTED_STATE_STORE_NAME_KEY = "jobs.mapping.distributed.state.store.name";
  public static final String DEFAULT_DISTRIBUTED_STATE_STORE_NAME = "distributedState";

  private StateStore<State> stateStore;
  private String distributedStateStoreName;

  public HelixJobsMapping(Config sysConfig, URI fsUri, String rootDir) {
    String stateStoreType = ConfigUtils.getString(sysConfig,
                                                  ConfigurationKeys.INTERMEDIATE_STATE_STORE_TYPE_KEY,
                                                  ConfigUtils.getString(sysConfig,
                                                                        ConfigurationKeys.STATE_STORE_TYPE_KEY,
                                                                        ConfigurationKeys.DEFAULT_STATE_STORE_TYPE));

    ClassAliasResolver<StateStore.Factory> resolver =
        new ClassAliasResolver<>(StateStore.Factory.class);
    StateStore.Factory stateStoreFactory;

    try {
      stateStoreFactory = resolver.resolveClass(stateStoreType).newInstance();
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }

    String dbTableKey = ConfigUtils.getString(sysConfig, JOBS_MAPPING_DB_TABLE_KEY, DEFAULT_JOBS_MAPPING_DB_TABLE_KEY_NAME);
    this.distributedStateStoreName = ConfigUtils.getString(sysConfig, DISTRIBUTED_STATE_STORE_NAME_KEY, DEFAULT_DISTRIBUTED_STATE_STORE_NAME);

    Config stateStoreJobConfig = sysConfig
        .withValue(ConfigurationKeys.STATE_STORE_FS_URI_KEY, ConfigValueFactory.fromAnyRef(fsUri.toString()))
        .withValue(ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY, ConfigValueFactory.fromAnyRef(rootDir))
        .withValue(ConfigurationKeys.STATE_STORE_DB_TABLE_KEY, ConfigValueFactory.fromAnyRef(dbTableKey));

    this.stateStore = stateStoreFactory.createStateStore(stateStoreJobConfig, State.class);
  }

  public static String createPlanningJobId (Properties jobPlanningProps) {
    return JobLauncherUtils.newJobId(GobblinClusterConfigurationKeys.PLANNING_JOB_NAME_PREFIX
        + JobState.getJobNameFromProps(jobPlanningProps));
  }

  public static String createActualJobId (Properties jobProps) {
    return JobLauncherUtils.newJobId(GobblinClusterConfigurationKeys.ACTUAL_JOB_NAME_PREFIX
        + JobState.getJobNameFromProps(jobProps));
  }

  @Nullable
  private State getOrCreate (String storeName, String jobName) throws IOException {
    if (this.stateStore.exists(storeName, jobName)) {
      return this.stateStore.get(storeName, jobName, jobName);
    }
    return new State();
  }

  public void deleteMapping (String jobName) throws IOException {
    this.stateStore.delete(this.distributedStateStoreName, jobName);
  }

  public void setPlanningJobId (String jobUri, String planningJobId) throws IOException {
    State state = getOrCreate(distributedStateStoreName, jobUri);
    state.setId(jobUri);
    state.setProp(GobblinClusterConfigurationKeys.PLANNING_ID_KEY, planningJobId);
    writeToStateStore(jobUri, state);
  }

  public void setActualJobId (String jobUri, String actualJobId) throws IOException {
    setActualJobId(jobUri, null, actualJobId);
  }

  public void setActualJobId (String jobUri, String planningJobId, String actualJobId) throws IOException {
    State state = getOrCreate(distributedStateStoreName, jobUri);
    state.setId(jobUri);
    if (null != planningJobId) {
      state.setProp(GobblinClusterConfigurationKeys.PLANNING_ID_KEY, planningJobId);
    }
    state.setProp(ConfigurationKeys.JOB_ID_KEY, actualJobId);
    writeToStateStore(jobUri, state);
  }

  public void setDistributedJobMode(String jobUri, boolean distributedJobMode) throws IOException {
    State state = getOrCreate(distributedStateStoreName, jobUri);
    state.setId(jobUri);
    state.setProp(GobblinClusterConfigurationKeys.DISTRIBUTED_JOB_LAUNCHER_ENABLED, distributedJobMode);
    writeToStateStore(jobUri, state);
  }

  private void writeToStateStore(String jobUri, State state) throws IOException {
    // fs state store use hdfs rename, which assumes the target file doesn't exist.
    if (this.stateStore instanceof FsStateStore) {
      this.deleteMapping(jobUri);
    }
    this.stateStore.put(distributedStateStoreName, jobUri, state);
  }

  private Optional<String> getId (String jobUri, String idName) throws IOException {
    State state = this.stateStore.get(distributedStateStoreName, jobUri, jobUri);
    if (state == null) {
      return Optional.empty();
    }

    String id = state.getProp(idName);

    return id == null ? Optional.empty() : Optional.of(id);
  }

  public List<State> getAllStates() throws IOException {
    return this.stateStore.getAll(distributedStateStoreName);
  }

  public Optional<String> getActualJobId (String jobUri) throws IOException {
    return getId(jobUri, ConfigurationKeys.JOB_ID_KEY);
  }

  public Optional<String> getPlanningJobId (String jobUri) throws IOException {
    return getId(jobUri, GobblinClusterConfigurationKeys.PLANNING_ID_KEY);
  }

  public Optional<String> getDistributedJobMode(String jobUri) throws IOException {
    State state = this.stateStore.get(distributedStateStoreName, jobUri, jobUri);
    if (state == null) {
      return Optional.empty();
    }

    String id = state.getProp(GobblinClusterConfigurationKeys.DISTRIBUTED_JOB_LAUNCHER_ENABLED);

    return id == null ? Optional.empty() : Optional.of(id);
  }
}
