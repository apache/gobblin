/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.cluster;

import gobblin.configuration.ConfigurationKeys;
import gobblin.metastore.StateStore;
import gobblin.runtime.TaskState;
import gobblin.source.workunit.MultiWorkUnit;
import gobblin.source.workunit.WorkUnit;
import gobblin.util.ClassAliasResolver;
import java.net.InetAddress;
import java.net.UnknownHostException;

import java.util.Properties;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import gobblin.annotation.Alpha;


@Alpha
public class GobblinClusterUtils {

  /**
   * Get the name of the current host.
   *
   * @return the name of the current host
   * @throws UnknownHostException if the host name is unknown
   */
  public static String getHostname() throws UnknownHostException {
    return InetAddress.getLocalHost().getHostName();
  }

  /**
   * Get the application working directory {@link Path}.
   *
   * @param fs a {@link FileSystem} instance on which {@link FileSystem#getHomeDirectory()} is called
   *           to get the home directory of the {@link FileSystem} of the application working directory
   * @param applicationName the application name
   * @param applicationId the application ID in string form
   * @return the cluster application working directory {@link Path}
   */
  public static Path getAppWorkDirPath(FileSystem fs, String applicationName, String applicationId) {
    return new Path(fs.getHomeDirectory(), getAppWorkDirPath(applicationName, applicationId));
  }

  /**
   * Get the application working directory {@link String}.
   *
   * @param applicationName the application name
   * @param applicationId the application ID in string form
   * @return the cluster application working directory {@link String}
   */
  public static String getAppWorkDirPath(String applicationName, String applicationId) {
    return applicationName + Path.SEPARATOR + applicationId;
  }

  /**
   * state stores used for storing work units and task states
   */
  public static class StateStores {
    public final StateStore<TaskState> taskStateStore;
    public final StateStore<WorkUnit> wuStateStore;
    public final StateStore<MultiWorkUnit> mwuStateStore;

    /**
     * Creates the state stores under appWorkDir
     * {@link WorkUnit}s will be stored under appWorkDir/_workunits/subdir/filename.(m)wu
     * {@link TaskState}s will be stored under appWorkDir/_taskstates/subdir/filename.tst
     * Some state stores such as the MysqlStateStore do not preserve the path prefix of appWorkDir.
     * In those cases only the last three components of the path determine the key for the data.
     * @param props properties
     * @param appWorkDir the base directory
     * @throws Exception
     */
    StateStores(Properties props, Path appWorkDir) throws Exception {
      String stateStoreType = props.getProperty(ConfigurationKeys.STATE_STORE_TYPE_KEY,
          ConfigurationKeys.DEFAULT_STATE_STORE_TYPE);

      ClassAliasResolver<StateStore.Factory> resolver =
          new ClassAliasResolver<>(StateStore.Factory.class);

      StateStore.Factory stateStoreFactory =
          resolver.resolveClass(stateStoreType).newInstance();

      // Override properties to configure the WorkUnit and MultiWorkUnit StateStores with the appropriate root/db location
      Properties wuStateStoreProps = new Properties(props);
      Path inputWorkUnitDir = new Path(appWorkDir, GobblinClusterConfigurationKeys.INPUT_WORK_UNIT_DIR_NAME);
      wuStateStoreProps.setProperty(ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY, inputWorkUnitDir.toString());
      wuStateStoreProps.setProperty(ConfigurationKeys.STATE_STORE_DB_TABLE_KEY,
          GobblinClusterConfigurationKeys.INPUT_WORK_UNIT_DIR_NAME);

      // Override properties to place the TaskState StateStore at the appropriate location
      Properties taskStateStoreProps = new Properties(props);
      Path taskStateOutputDir = new Path(appWorkDir, GobblinClusterConfigurationKeys.OUTPUT_TASK_STATE_DIR_NAME);
      taskStateStoreProps.setProperty(ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY, taskStateOutputDir.toString());
      taskStateStoreProps.setProperty(ConfigurationKeys.STATE_STORE_DB_TABLE_KEY,
          GobblinClusterConfigurationKeys.OUTPUT_TASK_STATE_DIR_NAME);

      taskStateStore = stateStoreFactory.createStateStore(taskStateStoreProps, TaskState.class);
      wuStateStore = stateStoreFactory.createStateStore(wuStateStoreProps, WorkUnit.class);
      mwuStateStore = stateStoreFactory.createStateStore(wuStateStoreProps, MultiWorkUnit.class);
    }
  }
}
