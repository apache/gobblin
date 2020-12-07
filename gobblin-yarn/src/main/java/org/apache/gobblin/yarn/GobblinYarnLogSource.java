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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import com.typesafe.config.Config;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.util.PathUtils;
import org.apache.gobblin.util.filesystem.FileSystemSupplier;
import org.apache.gobblin.util.logs.LogCopier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ContainerId;


/**
 * A base class for container processes that are sources of Gobblin Yarn application logs.
 *
 * @author Yinan Li
 */
class GobblinYarnLogSource {
  private static final Splitter COMMA_SPLITTER = Splitter.on(',').omitEmptyStrings().trimResults();
  private static final Configuration AUTO_CLOSE_CONFIG = new Configuration();

  static {
    AUTO_CLOSE_CONFIG.setBoolean("fs.automatic.close", false);
  }

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
   * TODO: This is duplicated to the org.apache.gobblin.yarn.GobblinYarnAppLauncher#buildLogCopier(com.typesafe.config.Config, org.apache.hadoop.fs.Path, org.apache.hadoop.fs.Path)
   *
   * @param config the {@link Config} use to create the {@link LogCopier}
   * @param containerId the {@link ContainerId} of the container the {@link LogCopier} runs in
   * @param destFs the destination {@link FileSystem}
   * @param appWorkDir the Gobblin Yarn application working directory on HDFS
   * @return a {@link LogCopier} instance
   * @throws IOException if it fails on any IO operation
   */
  protected LogCopier buildLogCopier(Config config, String containerId, FileSystem destFs, Path appWorkDir)
      throws IOException {
    LogCopier.Builder builder = LogCopier.newBuilder()
        .useDestFsSupplier(new FileSystemSupplier() {
          @Override
          public FileSystem getFileSystem() throws IOException {
            return buildFileSystem(config, false);
          }
        })
        .useSrcFsSupplier(new FileSystemSupplier() {
          @Override
          public FileSystem getFileSystem() throws IOException {
            return buildFileSystem(config, true);
          }
        })
        .readFrom(getLocalLogDirs())
        .writeTo(getHdfsLogDir(containerId, destFs, appWorkDir))
        .useCurrentLogFileName(Files.getNameWithoutExtension(
            System.getProperty(GobblinYarnConfigurationKeys.GOBBLIN_YARN_CONTAINER_LOG_FILE_NAME)));

    builder.acceptsLogFileExtensions(
        config.hasPath(GobblinYarnConfigurationKeys.LOG_FILE_EXTENSIONS) ? ImmutableSet.copyOf(
            Splitter.on(",").splitToList(config.getString(GobblinYarnConfigurationKeys.LOG_FILE_EXTENSIONS)))
            : ImmutableSet.of());

    return builder.build();
  }

  /**
   * Return a new (non-cached) {@link FileSystem} instance. The {@link FileSystem} instance
   * returned by the method has automatic closing disabled. The user of the instance needs to handle closing of the
   * instance, typically as part of its shutdown sequence.
   */
  public static FileSystem buildFileSystem(Config config, boolean isLocal) throws IOException {
    return isLocal ? FileSystem.newInstanceLocal(AUTO_CLOSE_CONFIG)
        : config.hasPath(ConfigurationKeys.FS_URI_KEY) ? FileSystem.newInstance(
            URI.create(config.getString(ConfigurationKeys.FS_URI_KEY)), AUTO_CLOSE_CONFIG)
            : FileSystem.newInstance(AUTO_CLOSE_CONFIG);
  }

  /**
   * Multiple directories may be specified in the LOG_DIRS string. Split them up and return a list of {@link Path}s.
   * @return list of {@link Path}s to the log directories
   * @throws IOException
   */
  private List<Path> getLocalLogDirs() throws IOException {
    String logDirs = System.getenv(ApplicationConstants.Environment.LOG_DIRS.toString());
    return COMMA_SPLITTER.splitToList(logDirs).stream().map(e -> new Path(e)).collect(Collectors.toList());
  }

  private Path getHdfsLogDir(String containerId, FileSystem destFs, Path appWorkDir) throws IOException {
    Path logRootDir =
        PathUtils.combinePaths(appWorkDir.toString(), GobblinYarnConfigurationKeys.APP_LOGS_DIR_NAME, containerId);
    if (!destFs.exists(logRootDir)) {
      destFs.mkdirs(logRootDir);
    }
    return logRootDir;
  }
}
