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
package org.apache.gobblin.runtime.util;

import java.util.Map;

import org.apache.hadoop.fs.Path;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metastore.StateStore;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.runtime.TaskState;
import org.apache.gobblin.source.workunit.MultiWorkUnit;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.util.ClassAliasResolver;
import org.apache.gobblin.util.ConfigUtils;

import lombok.Getter;

/**
 * state stores used for storing work units and task states
 */
public class StateStores {
  @Getter
  private final StateStore<TaskState> taskStateStore;
  @Getter
  private final StateStore<WorkUnit> wuStateStore;
  @Getter
  private final StateStore<MultiWorkUnit> mwuStateStore;
  // state store for job.state files. This should not be confused with the jst state store
  @Getter
  private final StateStore<JobState> jobStateStore;

  /**
   * Creates the state stores under storeBase
   * {@link WorkUnit}s will be stored under storeBase/_workunits/subdir/filename.(m)wu
   * {@link TaskState}s will be stored under storeBase/_taskstates/subdir/filename.tst
   * {@link JobState}s will be stored under StoreBase/_jobStates/subdir/filename.job.state
   * Some state stores such as the MysqlStateStore do not preserve the path prefix of storeRoot.
   * In those cases only the last three components of the path determine the key for the data.
   * @param config config properties
   * @param taskStoreBase the base directory that holds the store root for the task state store
   */
  public StateStores(Config config, Path taskStoreBase, String taskStoreTable, Path workUnitStoreBase,
      String workUnitStoreTable, Path jobStateStoreBase, String jobStateStoreTable) {
    String stateStoreType = ConfigUtils.getString(config, ConfigurationKeys.INTERMEDIATE_STATE_STORE_TYPE_KEY,
        ConfigUtils.getString(config, ConfigurationKeys.STATE_STORE_TYPE_KEY,
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

    // Override properties to configure the WorkUnit and MultiWorkUnit StateStores with the appropriate root/db location
    Path inputWorkUnitDir = new Path(workUnitStoreBase, workUnitStoreTable);
    Config wuStateStoreConfig = getStateStoreConfig(config, inputWorkUnitDir.toString(), workUnitStoreTable);

    // Override properties to place the TaskState StateStore at the appropriate location
    Path taskStateOutputDir = new Path(taskStoreBase, taskStoreTable);
    Config taskStateStoreConfig = getStateStoreConfig(config, taskStateOutputDir.toString(), taskStoreTable);

    taskStateStore = stateStoreFactory.createStateStore(taskStateStoreConfig, TaskState.class);
    wuStateStore = stateStoreFactory.createStateStore(wuStateStoreConfig, WorkUnit.class);
    mwuStateStore = stateStoreFactory.createStateStore(wuStateStoreConfig, MultiWorkUnit.class);

    // create a state store to store job.state content if configured
    if (ConfigUtils.getBoolean(config, ConfigurationKeys.JOB_STATE_IN_STATE_STORE,
        ConfigurationKeys.DEFAULT_JOB_STATE_IN_STATE_STORE)) {
      // Override properties to place the JobState StateStore at the appropriate location
      Path jobStateOutputDir = new Path(jobStateStoreBase, jobStateStoreTable);
      Config jobStateStoreConfig = getStateStoreConfig(config, jobStateOutputDir.toString(), jobStateStoreTable);

      jobStateStore = stateStoreFactory.createStateStore(jobStateStoreConfig, JobState.class);
    } else {
      jobStateStore = null;
    }
  }

  /**
   * @return true if a state store is present for storing job.state content
   */
  public boolean haveJobStateStore() {
    return this.jobStateStore != null;
  }

  private static Config getStateStoreConfig(Config config, String rootDir, String dbTableKey) {
    Config fallbackConfig = ConfigFactory.empty()
            .withFallback(config)
            .withValue(ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY, ConfigValueFactory.fromAnyRef(rootDir))
            .withValue(ConfigurationKeys.STATE_STORE_DB_TABLE_KEY, ConfigValueFactory.fromAnyRef(dbTableKey));
    Config scopedConfig = ConfigFactory.empty();
    for (Map.Entry<String, ConfigValue> entry : config.withOnlyPath(ConfigurationKeys.INTERMEDIATE_STATE_STORE_PREFIX).entrySet()) {
      scopedConfig.withValue(entry.getKey().substring(ConfigurationKeys.INTERMEDIATE_STATE_STORE_PREFIX.length()),
              entry.getValue());
    }
    return scopedConfig.withFallback(fallbackConfig);
  }
}