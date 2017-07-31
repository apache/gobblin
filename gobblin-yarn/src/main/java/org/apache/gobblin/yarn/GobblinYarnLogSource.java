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

package org.apache.gobblin.yarn;

import java.io.IOException;

import com.typesafe.config.Config;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ContainerId;

import com.google.common.collect.ImmutableSet;

import org.apache.gobblin.util.logs.LogCopier;


/**
 * A base class for container processes that are sources of Gobblin Yarn application logs.
 *
 * <p>
 *   The source log files are supposed to be on the local {@link FileSystem} and will
 *   be copied to a given destination {@link FileSystem}, which is typically HDFS.
 * </p>
 *
 * @author Yinan Li
 */
class GobblinYarnLogSource {

  /**
   * Return if the log source is present or not.
   *
   * @return {@code true} if the log source is present and {@code false} otherwise
   */
  protected boolean isLogSourcePresent() {
    return System.getenv().containsKey(ApplicationConstants.Environment.LOG_DIRS.toString());
  }

  /**
   * Build a {@link LogCopier} instance used to copy the logs out from this {@link GobblinYarnLogSource}.
   *
   * @param config the {@link Config} use to create the {@link LogCopier}
   * @param containerId the {@link ContainerId} of the container the {@link LogCopier} runs in
   * @param destFs the destination {@link FileSystem}
   * @param appWorkDir the Gobblin Yarn application working directory on HDFS
   * @return a {@link LogCopier} instance
   * @throws IOException if it fails on any IO operation
   */
  protected LogCopier buildLogCopier(Config config, ContainerId containerId, FileSystem destFs, Path appWorkDir)
      throws IOException {
    LogCopier.Builder builder = LogCopier.newBuilder()
            .useSrcFileSystem(FileSystem.getLocal(new Configuration()))
            .useDestFileSystem(destFs)
            .readFrom(getLocalLogDir())
            .writeTo(getHdfsLogDir(containerId, destFs, appWorkDir))
            .acceptsLogFileExtensions(ImmutableSet.of(ApplicationConstants.STDOUT, ApplicationConstants.STDERR))
            .useLogFileNamePrefix(containerId.toString());
    if (config.hasPath(GobblinYarnConfigurationKeys.LOG_COPIER_MAX_FILE_SIZE)) {
      builder.useMaxBytesPerLogFile(config.getBytes(GobblinYarnConfigurationKeys.LOG_COPIER_MAX_FILE_SIZE));
    }
    if (config.hasPath(GobblinYarnConfigurationKeys.LOG_COPIER_SCHEDULER)) {
      builder.useScheduler(config.getString(GobblinYarnConfigurationKeys.LOG_COPIER_SCHEDULER));
    }
    return builder.build();
  }

  private Path getLocalLogDir() throws IOException {
    return new Path(System.getenv(ApplicationConstants.Environment.LOG_DIRS.toString()));
  }

  private Path getHdfsLogDir(ContainerId containerId, FileSystem destFs, Path appWorkDir) throws IOException {
    Path logRootDir = new Path(appWorkDir, GobblinYarnConfigurationKeys.APP_LOGS_DIR_NAME);
    if (!destFs.exists(logRootDir)) {
      destFs.mkdirs(logRootDir);
    }

    return new Path(logRootDir, containerId.toString());
  }
}
