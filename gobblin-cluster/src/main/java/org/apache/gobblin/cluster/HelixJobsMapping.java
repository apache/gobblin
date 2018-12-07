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
import java.util.Optional;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;

import javax.annotation.Nullable;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.metastore.StateStore;
import org.apache.gobblin.util.ClassAliasResolver;
import org.apache.gobblin.util.ConfigUtils;


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

  public static final String DB_TABLE_KEY = "JobsMapping";

  private StateStore stateStore;

  public HelixJobsMapping(Config sysConfig, URI fsUri, String rootDir) {
    String stateStoreType = ConfigUtils.getString(sysConfig, ConfigurationKeys.INTERMEDIATE_STATE_STORE_TYPE_KEY,
        ConfigUtils.getString(sysConfig, ConfigurationKeys.STATE_STORE_TYPE_KEY,
            ConfigurationKeys.DEFAULT_STATE_STORE_TYPE));

    ClassAliasResolver<StateStore.Factory> resolver =
        new ClassAliasResolver<>(StateStore.Factory.class);
    StateStore.Factory stateStoreFactory;

    try {
      stateStoreFactory = resolver.resolveClass(stateStoreType).newInstance();
    } catch (ClassNotFoundException cnfe) {
      throw new RuntimeException(cnfe);
    } catch (InstantiationException ie) {
      throw new RuntimeException(ie);
    } catch (IllegalAccessException iae) {
      throw new RuntimeException(iae);
    }

    Config stateStoreJobConfig = sysConfig
        .withValue(ConfigurationKeys.STATE_STORE_FS_URI_KEY, ConfigValueFactory.fromAnyRef(fsUri.toString()))
        .withValue(ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY, ConfigValueFactory.fromAnyRef(rootDir))
        .withValue(ConfigurationKeys.STATE_STORE_DB_TABLE_KEY, ConfigValueFactory.fromAnyRef(DB_TABLE_KEY));

    this.stateStore = stateStoreFactory.createStateStore(stateStoreJobConfig, State.class);
  }

  @Nullable
  private State getOrCreate (String jobName) throws IOException {
    if (this.stateStore.exists(jobName, jobName)) {
      return this.stateStore.get(jobName, jobName, jobName);
    }
    return new State();
  }

  public void setPlanningJobId (String jobName, String planningJobId) throws IOException {
    State state = getOrCreate(jobName);
    state.setId(jobName);
    state.setProp(GobblinClusterConfigurationKeys.PLANNING_ID_KEY, planningJobId);
    this.stateStore.put(jobName, jobName, state);
  }

  public void setActualJobId (String jobName, String planningJobId, String actualJobId) throws IOException {
    State state = getOrCreate(jobName);
    state.setId(jobName);
    state.setProp(GobblinClusterConfigurationKeys.PLANNING_ID_KEY, planningJobId);
    state.setProp(ConfigurationKeys.JOB_ID_KEY, actualJobId);
    this.stateStore.put(jobName, jobName, state);
  }

  private Optional<String> getId (String jobName, String idName) throws IOException {
    State state = this.stateStore.get(jobName, jobName, jobName);
    if (state == null) {
      return Optional.empty();
    }

    String id = state.getProp(idName);

    return id == null? Optional.empty() : Optional.of(id);
  }

  public Optional<String> getActualJobId (String jobName) throws IOException {
    return getId(jobName, ConfigurationKeys.JOB_ID_KEY);
  }

  public Optional<String> getPlanningJobId (String jobName) throws IOException {
    return getId(jobName, GobblinClusterConfigurationKeys.PLANNING_ID_KEY);
  }
}
