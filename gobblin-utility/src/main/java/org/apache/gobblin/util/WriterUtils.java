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

package org.apache.gobblin.util;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.ExecutionException;

import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.token.Token;

import com.github.rholder.retry.Retryer;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.util.retry.RetryerFactory;


/**
 * Utility class for use with the {@link org.apache.gobblin.writer.DataWriter} class.
 */
@Slf4j
public class WriterUtils {
  public static final String WRITER_ENCRYPTED_CONFIG_PATH = ConfigurationKeys.WRITER_PREFIX + ".encrypted";

  public static final Config NO_RETRY_CONFIG = ConfigFactory.empty();

  public enum WriterFilePathType {
    /**
     * Write records into namespace/table folder. If namespace has multiple components, each component will be
     * a folder in the path. For example: the write file path for namespace 'org.apache.gobblin' and table 'tableName'
     * will be 'org/apache/gobblin/tableName'
     */
    NAMESPACE_TABLE,
    /**
     * TABLENAME should be used for jobs that pull from multiple tables/topics and intend to write the records
     * in each table/topic to a separate folder.
     */
    TABLENAME,
    /** Write records into the output file decided by {@link org.apache.gobblin.source.workunit.Extract}*/
    DEFAULT
  }

  /**
   * Get the {@link Path} corresponding the to the directory a given {@link org.apache.gobblin.writer.DataWriter} should be writing
   * its staging data. The staging data directory is determined by combining the
   * {@link ConfigurationKeys#WRITER_STAGING_DIR} and the {@link ConfigurationKeys#WRITER_FILE_PATH}.
   * @param state is the {@link State} corresponding to a specific {@link org.apache.gobblin.writer.DataWriter}.
   * @param numBranches is the total number of branches for the given {@link State}.
   * @param branchId is the id for the specific branch that the {@link org.apache.gobblin.writer.DataWriter} will write to.
   * @return a {@link Path} specifying the directory where the {@link org.apache.gobblin.writer.DataWriter} will write to.
   */
  public static Path getWriterStagingDir(State state, int numBranches, int branchId) {
    String writerStagingDirKey =
        ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_STAGING_DIR, numBranches, branchId);
    Preconditions.checkArgument(state.contains(writerStagingDirKey),
        "Missing required property " + writerStagingDirKey);

    return new Path(
        state.getProp(
            ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_STAGING_DIR, numBranches, branchId)),
        WriterUtils.getWriterFilePath(state, numBranches, branchId));
  }

  /**
   * Get the staging {@link Path} for {@link org.apache.gobblin.writer.DataWriter} that has attemptId in the path.
   */
  public static Path getWriterStagingDir(State state, int numBranches, int branchId, String attemptId) {
    Preconditions.checkArgument(attemptId != null && !attemptId.isEmpty(), "AttemptId cannot be null or empty: " + attemptId);
    return new Path(getWriterStagingDir(state, numBranches, branchId), attemptId);
  }

  /**
   * Get the {@link Path} corresponding the to the directory a given {@link org.apache.gobblin.writer.DataWriter} should be writing
   * its output data. The output data directory is determined by combining the
   * {@link ConfigurationKeys#WRITER_OUTPUT_DIR} and the {@link ConfigurationKeys#WRITER_FILE_PATH}.
   * @param state is the {@link State} corresponding to a specific {@link org.apache.gobblin.writer.DataWriter}.
   * @param numBranches is the total number of branches for the given {@link State}.
   * @param branchId is the id for the specific branch that the {@link org.apache.gobblin.writer.DataWriter} will write to.
   * @return a {@link Path} specifying the directory where the {@link org.apache.gobblin.writer.DataWriter} will write to.
   */
  public static Path getWriterOutputDir(State state, int numBranches, int branchId) {
    String writerOutputDirKey =
        ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_OUTPUT_DIR, numBranches, branchId);
    Preconditions.checkArgument(state.contains(writerOutputDirKey), "Missing required property " + writerOutputDirKey);

    return new Path(state.getProp(writerOutputDirKey), WriterUtils.getWriterFilePath(state, numBranches, branchId));
  }

  /**
   * Get the {@link Path} corresponding the to the directory a given {@link org.apache.gobblin.publisher.BaseDataPublisher} should
   * commits its output data. The final output data directory is determined by combining the
   * {@link ConfigurationKeys#DATA_PUBLISHER_FINAL_DIR} and the {@link ConfigurationKeys#WRITER_FILE_PATH}.
   * @param state is the {@link State} corresponding to a specific {@link org.apache.gobblin.writer.DataWriter}.
   * @param numBranches is the total number of branches for the given {@link State}.
   * @param branchId is the id for the specific branch that the {@link org.apache.gobblin.publisher.BaseDataPublisher} will publish.
   * @return a {@link Path} specifying the directory where the {@link org.apache.gobblin.publisher.BaseDataPublisher} will publish.
   */
  public static Path getDataPublisherFinalDir(State state, int numBranches, int branchId) {
    String dataPublisherFinalDirKey =
        ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, numBranches, branchId);
    Preconditions.checkArgument(state.contains(dataPublisherFinalDirKey),
        "Missing required property " + dataPublisherFinalDirKey);

    if (state.getPropAsBoolean(ConfigurationKeys.DATA_PUBLISHER_APPEND_EXTRACT_TO_FINAL_DIR,
        ConfigurationKeys.DEFAULT_DATA_PUBLISHER_APPEND_EXTRACT_TO_FINAL_DIR)) {
      return new Path(state.getProp(
          ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, numBranches, branchId)),
          WriterUtils.getWriterFilePath(state, numBranches, branchId));
    } else {
      return new Path(state.getProp(
          ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, numBranches, branchId)));
    }
  }

  /**
   * Get the {@link Path} corresponding the the relative file path for a given {@link org.apache.gobblin.writer.DataWriter}.
   * This method retrieves the value of {@link ConfigurationKeys#WRITER_FILE_PATH} from the given {@link State}. It also
   * constructs the default value of the {@link ConfigurationKeys#WRITER_FILE_PATH} if not is not specified in the given
   * {@link State}.
   * @param state is the {@link State} corresponding to a specific {@link org.apache.gobblin.writer.DataWriter}.
   * @param numBranches is the total number of branches for the given {@link State}.
   * @param branchId is the id for the specific branch that the {{@link org.apache.gobblin.writer.DataWriter} will write to.
   * @return a {@link Path} specifying the relative directory where the {@link org.apache.gobblin.writer.DataWriter} will write to.
   */
  public static Path getWriterFilePath(State state, int numBranches, int branchId) {
    if (state.contains(
        ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_FILE_PATH, numBranches, branchId))) {
      return new Path(state.getProp(
          ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_FILE_PATH, numBranches, branchId)));
    }

    switch (getWriterFilePathType(state)) {
      case NAMESPACE_TABLE:
        return getNamespaceTableWriterFilePath(state);
      case TABLENAME:
        return WriterUtils.getTableNameWriterFilePath(state);
      default:
        return WriterUtils.getDefaultWriterFilePath(state, numBranches, branchId);
    }
  }

  private static WriterFilePathType getWriterFilePathType(State state) {
    String pathTypeStr =
        state.getProp(ConfigurationKeys.WRITER_FILE_PATH_TYPE, ConfigurationKeys.DEFAULT_WRITER_FILE_PATH_TYPE);
    return WriterFilePathType.valueOf(pathTypeStr.toUpperCase());
  }

  /**
   * Creates {@link Path} for case {@link WriterFilePathType#NAMESPACE_TABLE} with configurations
   * {@link ConfigurationKeys#EXTRACT_NAMESPACE_NAME_KEY} and {@link ConfigurationKeys#EXTRACT_TABLE_NAME_KEY}
   * @param state
   * @return a path
   */
  public static Path getNamespaceTableWriterFilePath(State state) {
    Preconditions.checkArgument(state.contains(ConfigurationKeys.EXTRACT_NAMESPACE_NAME_KEY));
    Preconditions.checkArgument(state.contains(ConfigurationKeys.EXTRACT_TABLE_NAME_KEY));

    String namespace = state.getProp(ConfigurationKeys.EXTRACT_NAMESPACE_NAME_KEY).replaceAll("\\.", Path.SEPARATOR);
    return new Path( namespace + Path.SEPARATOR + state.getProp(ConfigurationKeys.EXTRACT_TABLE_NAME_KEY));
  }

  /**
   * Creates {@link Path} for the {@link ConfigurationKeys#WRITER_FILE_PATH} key according to
   * {@link ConfigurationKeys#EXTRACT_TABLE_NAME_KEY}.
   * @param state
   * @return
   */
  public static Path getTableNameWriterFilePath(State state) {
    Preconditions.checkArgument(state.contains(ConfigurationKeys.EXTRACT_TABLE_NAME_KEY));
    return new Path(state.getProp(ConfigurationKeys.EXTRACT_TABLE_NAME_KEY));
  }

  /**
   * Creates the default {@link Path} for the {@link ConfigurationKeys#WRITER_FILE_PATH} key.
   * @param numBranches is the total number of branches for the given {@link State}.
   * @param branchId is the id for the specific branch that the {@link org.apache.gobblin.writer.DataWriter} will write to.
   * @return a {@link Path} specifying the directory where the {@link org.apache.gobblin.writer.DataWriter} will write to.
   */
  public static Path getDefaultWriterFilePath(State state, int numBranches, int branchId) {
    if (state instanceof WorkUnitState) {
      WorkUnitState workUnitState = (WorkUnitState) state;
      return new Path(ForkOperatorUtils.getPathForBranch(workUnitState, workUnitState.getExtract().getOutputFilePath(),
          numBranches, branchId));

    } else if (state instanceof WorkUnit) {
      WorkUnit workUnit = (WorkUnit) state;
      return new Path(ForkOperatorUtils.getPathForBranch(workUnit, workUnit.getExtract().getOutputFilePath(),
          numBranches, branchId));
    }

    throw new RuntimeException("In order to get the default value for " + ConfigurationKeys.WRITER_FILE_PATH
        + " the given state must be of type " + WorkUnitState.class.getName() + " or " + WorkUnit.class.getName());
  }

  /**
   * Get the value of {@link ConfigurationKeys#WRITER_FILE_NAME} for the a given {@link org.apache.gobblin.writer.DataWriter}. The
   * method also constructs the default value of the {@link ConfigurationKeys#WRITER_FILE_NAME} if it is not set in the
   * {@link State}
   * @param state is the {@link State} corresponding to a specific {@link org.apache.gobblin.writer.DataWriter}.
   * @param numBranches is the total number of branches for the given {@link State}.
   * @param branchId is the id for the specific branch that the {{@link org.apache.gobblin.writer.DataWriter} will write to.
   * @param writerId is the id for a specific {@link org.apache.gobblin.writer.DataWriter}.
   * @param formatExtension is the format extension for the file (e.g. ".avro").
   * @return a {@link String} representation of the file name.
   */
  public static String getWriterFileName(State state, int numBranches, int branchId, String writerId,
      String formatExtension) {
    String defaultFileName = Strings.isNullOrEmpty(formatExtension)
        ? String.format("%s.%s", ConfigurationKeys.DEFAULT_WRITER_FILE_BASE_NAME, writerId)
        : String.format("%s.%s.%s", ConfigurationKeys.DEFAULT_WRITER_FILE_BASE_NAME, writerId, formatExtension);
    return state.getProp(
        ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_FILE_NAME, numBranches, branchId),
        defaultFileName);
  }

  /**
   * Creates a {@link CodecFactory} based on the specified codec name and deflate level. If codecName is absent, then
   * a {@link CodecFactory#deflateCodec(int)} is returned. Otherwise the codecName is converted into a
   * {@link CodecFactory} via the {@link CodecFactory#fromString(String)} method.
   *
   * @param codecName the name of the codec to use (e.g. deflate, snappy, xz, etc.).
   * @param deflateLevel must be an integer from [0-9], and is only applicable if the codecName is "deflate".
   * @return a {@link CodecFactory}.
   */
  public static CodecFactory getCodecFactory(Optional<String> codecName, Optional<String> deflateLevel) {
    if (!codecName.isPresent()) {
      return CodecFactory.deflateCodec(ConfigurationKeys.DEFAULT_DEFLATE_LEVEL);
    } else if (codecName.get().equalsIgnoreCase(DataFileConstants.DEFLATE_CODEC)) {
      if (!deflateLevel.isPresent()) {
        return CodecFactory.deflateCodec(ConfigurationKeys.DEFAULT_DEFLATE_LEVEL);
      }
      return CodecFactory.deflateCodec(Integer.parseInt(deflateLevel.get()));
    } else {
      return CodecFactory.fromString(codecName.get().toLowerCase());
    }
  }

  /**
   * Create the given dir as well as all missing ancestor dirs. All created dirs will have the given permission.
   * This should be used instead of {@link FileSystem#mkdirs(Path, FsPermission)}, since that method only sets
   * the permission for the given dir, and not recursively for the ancestor dirs.
   *
   * @param fs FileSystem
   * @param path The dir to be created
   * @param perm The permission to be set
   * @throws IOException if failing to create dir or set permission.
   */
  public static void mkdirsWithRecursivePermission(final FileSystem fs, final Path path, FsPermission perm) throws IOException {
    mkdirsWithRecursivePermissionWithRetry(fs, path, perm, NO_RETRY_CONFIG);
  }

  public static void mkdirsWithRecursivePermissionWithRetry(final FileSystem fs, final Path path, FsPermission perm, Config retrierConfig) throws IOException {

    if (fs.exists(path)) {
      return;
    }

    if (path.getParent() != null && !fs.exists(path.getParent())) {
      mkdirsWithRecursivePermissionWithRetry(fs, path.getParent(), perm, retrierConfig);
    }

    if (!fs.mkdirs(path, perm)) {
      throw new IOException(String.format("Unable to mkdir %s with permission %s", path, perm));
    }

    if (retrierConfig != NO_RETRY_CONFIG) {
      //Wait until file is not there as it can happen the file fail to exist right away on eventual consistent fs like Amazon S3
      Retryer<Void> retryer = RetryerFactory.newInstance(retrierConfig);

      try {
        retryer.call(() -> {
          if (!fs.exists(path)) {
            throw new IOException("Path " + path + " does not exist however it should. Will wait more.");
          }
          return null;
        });
      } catch (Exception e) {
        throw new IOException("Path " + path + "does not exist however it should. Giving up..."+ e);
      }
    }

    // Double check permission, since fs.mkdirs() may not guarantee to set the permission correctly
    if (!fs.getFileStatus(path).getPermission().equals(perm)) {
      fs.setPermission(path, perm);
    }
  }

  public static FileSystem getWriterFS(State state, int numBranches, int branchId)
      throws IOException {
    URI uri = URI.create(state.getProp(
        ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_FILE_SYSTEM_URI, numBranches, branchId),
        ConfigurationKeys.LOCAL_FS_URI));

    Configuration hadoopConf = getFsConfiguration(state);
    if (state.getPropAsBoolean(ConfigurationKeys.SHOULD_FS_PROXY_AS_USER,
        ConfigurationKeys.DEFAULT_SHOULD_FS_PROXY_AS_USER)) {
      // Initialize file system for a proxy user.
      String authMethod =
          state.getProp(ConfigurationKeys.FS_PROXY_AUTH_METHOD, ConfigurationKeys.DEFAULT_FS_PROXY_AUTH_METHOD);
      if (authMethod.equalsIgnoreCase(ConfigurationKeys.TOKEN_AUTH)) {
        return getWriterFsUsingToken(state, uri);
      } else if (authMethod.equalsIgnoreCase(ConfigurationKeys.KERBEROS_AUTH)) {
        return getWriterFsUsingKeytab(state, uri);
      }
    }
    // Initialize file system as the current user.
    return FileSystem.get(uri, hadoopConf);
  }

  public static FileSystem getWriterFs(State state)
      throws IOException {
    return getWriterFS(state, 1, 0);
  }

  private static FileSystem getWriterFsUsingToken(State state, URI uri)
      throws IOException {
    try {
      String user = state.getProp(ConfigurationKeys.FS_PROXY_AS_USER_NAME);
      Optional<Token<?>> token = ProxiedFileSystemUtils
          .getTokenFromSeqFile(user, new Path(state.getProp(ConfigurationKeys.FS_PROXY_AS_USER_TOKEN_FILE)));
      if (!token.isPresent()) {
        throw new IOException("No token found for user " + user);
      }
      return ProxiedFileSystemCache.fromToken().userNameToken(token.get())
          .userNameToProxyAs(state.getProp(ConfigurationKeys.FS_PROXY_AS_USER_NAME)).fsURI(uri)
          .conf(HadoopUtils.newConfiguration()).build();
    } catch (ExecutionException e) {
      throw new IOException(e);
    }
  }

  private static FileSystem getWriterFsUsingKeytab(State state, URI uri)
      throws IOException {
    FileSystem fs = FileSystem.newInstance(uri, new Configuration());
    try {
      Preconditions.checkArgument(state.contains(ConfigurationKeys.FS_PROXY_AS_USER_NAME),
          "Missing required property " + ConfigurationKeys.FS_PROXY_AS_USER_NAME);
      Preconditions.checkArgument(state.contains(ConfigurationKeys.SUPER_USER_NAME_TO_PROXY_AS_OTHERS),
          "Missing required property " + ConfigurationKeys.SUPER_USER_NAME_TO_PROXY_AS_OTHERS);
      Preconditions.checkArgument(state.contains(ConfigurationKeys.SUPER_USER_KEY_TAB_LOCATION),
          "Missing required property " + ConfigurationKeys.SUPER_USER_KEY_TAB_LOCATION);
      String user = state.getProp(ConfigurationKeys.FS_PROXY_AS_USER_NAME);
      String superUser = state.getProp(ConfigurationKeys.SUPER_USER_NAME_TO_PROXY_AS_OTHERS);
      Path keytabLocation = new Path(state.getProp(ConfigurationKeys.SUPER_USER_KEY_TAB_LOCATION));
      return ProxiedFileSystemCache.fromKeytab().userNameToProxyAs(user).fsURI(uri)
          .superUserKeytabLocation(keytabLocation).superUserName(superUser).conf(HadoopUtils.newConfiguration())
          .referenceFS(fs).build();
    } catch (ExecutionException e) {
      throw new IOException(e);
    }
  }

  public static Configuration getFsConfiguration(State state) {
    return HadoopUtils.getConfFromState(state, Optional.of(WRITER_ENCRYPTED_CONFIG_PATH));
  }
}
