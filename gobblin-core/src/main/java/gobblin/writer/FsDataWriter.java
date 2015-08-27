/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.writer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.io.Closer;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.util.FinalState;
import gobblin.util.ForkOperatorUtils;
import gobblin.util.HadoopUtils;
import gobblin.util.JobConfigurationUtils;
import gobblin.util.ProxiedFileSystemWrapper;
import gobblin.util.WriterUtils;


/**
 * An implementation of {@link DataWriter} does the work of setting the output/staging dir
 * and creating the FileSystem instance.
 *
 * @author akshay@nerdwallet.com
 */
public abstract class FsDataWriter<D> implements DataWriter<D>, FinalState {

  private static final Logger LOG = LoggerFactory.getLogger(FsDataWriter.class);

  protected final State properties;
  protected final FileSystem fs;
  protected final Path stagingFile;
  protected final Path outputFile;
  protected final int bufferSize;
  protected final short replicationFactor;
  protected final long blockSize;
  protected final FsPermission filePermission;
  protected final FsPermission dirPermission;
  protected final Optional<String> group;
  protected final OutputStream stagingFileOutputStream;
  protected final Closer closer = Closer.create();

  public FsDataWriter(State properties, String fileName, int numBranches, int branchId) throws IOException {
    this.properties = properties;

    Configuration conf = new Configuration();
    // Add all job configuration properties so they are picked up by Hadoop
    JobConfigurationUtils.putStateIntoConfiguration(properties, conf);

    String uri = properties.getProp(
        ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_FILE_SYSTEM_URI, numBranches, branchId),
        ConfigurationKeys.LOCAL_FS_URI);

    if (properties.getPropAsBoolean(ConfigurationKeys.SHOULD_FS_PROXY_AS_USER,
        ConfigurationKeys.DEFAULT_SHOULD_FS_PROXY_AS_USER)) {
      // Initialize file system as a proxy user.
      try {
        this.fs =
            new ProxiedFileSystemWrapper().getProxiedFileSystem(properties, ProxiedFileSystemWrapper.AuthType.TOKEN,
                properties.getProp(ConfigurationKeys.FS_PROXY_AS_USER_TOKEN_FILE), uri);
      } catch (InterruptedException e) {
        throw new IOException(e);
      } catch (URISyntaxException e) {
        throw new IOException(e);
      }
    } else {
      // Initialize file system as the current user.
      this.fs = FileSystem.get(URI.create(uri), conf);
    }

    // Initialize staging/output directory
    this.stagingFile = new Path(WriterUtils.getWriterStagingDir(properties, numBranches, branchId), fileName);
    this.outputFile = new Path(WriterUtils.getWriterOutputDir(properties, numBranches, branchId), fileName);
    this.properties.setProp(
        ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_FINAL_OUTPUT_PATH, branchId),
        this.outputFile.toString());

    // Deleting the staging file if it already exists, which can happen if the
    // task failed and the staging file didn't get cleaned up for some reason.
    // Deleting the staging file prevents the task retry from being blocked.
    if (this.fs.exists(this.stagingFile)) {
      LOG.warn(String.format("Task staging file %s already exists, deleting it", this.stagingFile));
      HadoopUtils.deletePath(this.fs, this.stagingFile, false);
    }

    this.bufferSize = Integer.parseInt(properties.getProp(
        ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_BUFFER_SIZE, numBranches, branchId),
        ConfigurationKeys.DEFAULT_BUFFER_SIZE));

    this.replicationFactor = properties.getPropAsShort(ForkOperatorUtils
        .getPropertyNameForBranch(ConfigurationKeys.WRITER_FILE_REPLICATION_FACTOR, numBranches, branchId),
        this.fs.getDefaultReplication(this.outputFile));

    this.blockSize = properties.getPropAsLong(
        ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_FILE_BLOCK_SIZE, numBranches, branchId),
        this.fs.getDefaultBlockSize(this.outputFile));

    this.filePermission = new FsPermission(properties.getPropAsShortWithRadix(
        ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_FILE_PERMISSIONS, numBranches, branchId),
        FsPermission.getDefault().toShort(), ConfigurationKeys.PERMISSION_PARSING_RADIX));

    this.dirPermission = new FsPermission(properties.getPropAsShortWithRadix(
        ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_DIR_PERMISSIONS, numBranches, branchId),
        FsPermission.getDefault().toShort(), ConfigurationKeys.PERMISSION_PARSING_RADIX));

    this.stagingFileOutputStream = this.closer.register(this.fs.create(this.stagingFile, this.filePermission, true,
        this.bufferSize, this.replicationFactor, this.blockSize, null));

    this.group = Optional.fromNullable(properties.getProp(ConfigurationKeys.WRITER_GROUP_NAME));
    if (this.group.isPresent()) {
      HadoopUtils.setGroup(this.fs, this.stagingFile, this.group.get());
    }

    // Create the parent directory of the output file if it does not exist
    WriterUtils.mkdirsWithRecursivePermission(this.fs, this.outputFile.getParent(), this.dirPermission);
  }

  /**
   * {@inheritDoc}.
   *
   * <p>
   *   This default implementation simply renames the staging file to the output file. If the output file
   *   already exists, it will delete it first before doing the renaming.
   * </p>
   *
   * @throws IOException if any file operation fails
   */
  @Override
  public void commit() throws IOException {
    this.close();

    if (!this.fs.exists(this.stagingFile)) {
      throw new IOException(String.format("File %s does not exist", this.stagingFile));
    }

    // Double check permission of staging file
    if (!this.fs.getFileStatus(this.stagingFile).getPermission().equals(this.filePermission)) {
      this.fs.setPermission(this.stagingFile, this.filePermission);
    }

    LOG.info(String.format("Moving data from %s to %s", this.stagingFile, this.outputFile));
    // For the same reason as deleting the staging file if it already exists, deleting
    // the output file if it already exists prevents task retry from being blocked.
    if (this.fs.exists(this.outputFile)) {
      LOG.warn(String.format("Task output file %s already exists", this.outputFile));
      HadoopUtils.deletePath(this.fs, this.outputFile, false);
    }

    HadoopUtils.renamePath(this.fs, this.stagingFile, this.outputFile);
  }

  /**
   * {@inheritDoc}.
   *
   * <p>
   *   This default implementation simply deletes the staging file if it exists.
   * </p>
   *
   * @throws IOException if deletion of the staging file fails
   */
  @Override
  public void cleanup() throws IOException {
    // Delete the staging file
    if (this.fs.exists(this.stagingFile)) {
      HadoopUtils.deletePath(this.fs, this.stagingFile, false);
    }
  }

  @Override
  public void close() throws IOException {
    this.closer.close();
  }

  @Override
  public State getFinalState() {
    State state = new State();

    state.setProp("RecordsWritten", recordsWritten());
    try {
      state.setProp("BytesWritten", bytesWritten());
    } catch (Exception exception) {
      // If Writer fails to return bytesWritten, it might not be implemented, or implemented incorrectly.
      // Omit property instead of failing.
    }

    return state;
  }
}
