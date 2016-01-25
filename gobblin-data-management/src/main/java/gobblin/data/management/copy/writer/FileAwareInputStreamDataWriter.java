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

package gobblin.data.management.copy.writer;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.data.management.copy.CopySource;
import gobblin.data.management.copy.CopyableDatasetMetadata;
import gobblin.data.management.copy.CopyableFile;
import gobblin.data.management.copy.FileAwareInputStream;
import gobblin.data.management.copy.OwnerAndPermission;
import gobblin.data.management.copy.PreserveAttributes;
import gobblin.data.management.copy.recovery.RecoveryHelper;
import gobblin.util.PathUtils;
import gobblin.util.FileListUtils;
import gobblin.util.ForkOperatorUtils;
import gobblin.util.HadoopUtils;
import gobblin.util.WriterUtils;
import gobblin.util.io.StreamUtils;
import gobblin.writer.DataWriter;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.io.Closer;


/**
 * A {@link DataWriter} to write {@link FileAwareInputStream}
 */
@Slf4j
public class FileAwareInputStreamDataWriter implements DataWriter<FileAwareInputStream> {

  protected final AtomicLong bytesWritten = new AtomicLong();
  protected final AtomicLong filesWritten = new AtomicLong();
  protected final State state;
  protected final FileSystem fs;
  protected final Path stagingDir;
  protected final Path outputDir;
  protected final Closer closer = Closer.create();
  protected CopyableDatasetMetadata copyableDatasetMetadata;
  protected final RecoveryHelper recoveryHelper;

  public FileAwareInputStreamDataWriter(State state, int numBranches, int branchId) throws IOException {
    this.state = state;

    String uri =
        this.state
            .getProp(ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_FILE_SYSTEM_URI, numBranches,
                branchId), ConfigurationKeys.LOCAL_FS_URI);

    this.fs = FileSystem.get(URI.create(uri), new Configuration());
    this.stagingDir = WriterUtils.getWriterStagingDir(state, numBranches, branchId);
    this.outputDir =
        new Path(state.getProp(ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_OUTPUT_DIR,
            numBranches, branchId)));
    this.copyableDatasetMetadata =
        CopyableDatasetMetadata.deserialize(state.getProp(CopySource.SERIALIZED_COPYABLE_DATASET));
    this.recoveryHelper = new RecoveryHelper(this.fs, state);
  }

  @Override
  public void write(FileAwareInputStream fileAwareInputStream) throws IOException {
    fileAwareInputStream.getInputStream();
    Path stagingFile = getStagingFilePath(fileAwareInputStream.getFile());
    CopyableFile copyableFile = fileAwareInputStream.getFile();

    this.fs.mkdirs(stagingFile.getParent());

    final short replication = copyableFile.getPreserve().preserve(PreserveAttributes.Option.REPLICATION) ?
        copyableFile.getOrigin().getReplication() : fs.getDefaultReplication(stagingFile);
    final long blockSize = copyableFile.getPreserve().preserve(PreserveAttributes.Option.BLOCK_SIZE) ?
        copyableFile.getOrigin().getBlockSize() : fs.getDefaultBlockSize(stagingFile);

    Predicate<FileStatus> fileStatusAttributesFilter = new Predicate<FileStatus>() {
      @Override public boolean apply(FileStatus input) {
        return input.getReplication() == replication && input.getBlockSize() == blockSize;
      }
    };
    Optional<FileStatus> persistedFile = this.recoveryHelper.findPersistedFile(this.state,
        fileAwareInputStream.getFile(), fileStatusAttributesFilter);

    if (persistedFile.isPresent()) {
      log.info(String.format("Recovering persisted file %s to %s.", persistedFile.get().getPath(), stagingFile));
      this.fs.rename(persistedFile.get().getPath(), stagingFile);
    } else {

      FSDataOutputStream os =
          this.fs.create(stagingFile, true, fs.getConf().getInt("io.file.buffer.size", 4096), replication, blockSize);
      try {
        this.bytesWritten.addAndGet(StreamUtils.copy(fileAwareInputStream.getInputStream(), os));
        log.info("bytes written: " + this.bytesWritten.get() + " for file " + fileAwareInputStream.getFile());
      } finally {
        os.close();
        fileAwareInputStream.getInputStream().close();
      }
    }

    this.filesWritten.incrementAndGet();

    try {
      setFilePermissions(fileAwareInputStream.getFile());
    } catch (IOException ioe) {
      log.error(String.format("Failed to set permissions for file %s. Attempting to persist it for future runs.",
          stagingFile));
      this.recoveryHelper.persistFile(this.state, fileAwareInputStream.getFile(), stagingFile);
    }
  }

  /**
   * Sets the owner/group and permission for the file in the task staging directory
   */
  protected void setFilePermissions(CopyableFile file) throws IOException {
    setAncestorPermissions(file);
    setRecursivePermission(getStagingFilePath(file), file.getDestinationOwnerAndPermission());
  }

  protected Path getStagingFilePath(CopyableFile file) {
    CopyableFile.DatasetAndPartition datasetAndPartition =
        file.getDatasetAndPartition(this.copyableDatasetMetadata);
    return new Path(new Path(this.stagingDir, datasetAndPartition.identifier()),
        PathUtils.withoutLeadingSeparator(file.getDestination()));
  }

  protected Path getOutputFilePath(CopyableFile file) {
    return new Path(this.outputDir, PathUtils.withoutLeadingSeparator(file.getDestination()));
  }

  /**
   * Uses the ancestor {@link OwnerAndPermission}s from {@link CopyableFile#getAncestorsOwnerAndPermission()} to walk
   * through the file's ancestors and sets the permissions
   */
  private void setAncestorPermissions(CopyableFile file) throws IOException {

    if (file.getAncestorsOwnerAndPermission() == null) {
      return;
    }
    Path parentPath = getStagingFilePath(file).getParent();
    for (OwnerAndPermission ownerAndPermission : file.getAncestorsOwnerAndPermission()) {
      if (parentPath == null) {
        log.info("Ancestor owner and permission may not be set correctly. Exhausted parent paths before ancestor permissions");
        log.info(String.format("File destination path %s, AncestorOwnerAndPermissions size %s.", file.getDestination(),
            file.getAncestorsOwnerAndPermission().size()));
        break;
      }
      safeSetPathPermission(parentPath, ownerAndPermission);
      parentPath = parentPath.getParent();
    }
  }

  /**
   * Sets the {@link FsPermission}, owner, group for the path passed. It will not throw exceptions, if operations
   * cannot be executed, will warn and continue.
   */
  private void safeSetPathPermission(Path path, OwnerAndPermission ownerAndPermission) {

    try {
      if (ownerAndPermission.getFsPermission() != null) {
        fs.setPermission(path, ownerAndPermission.getFsPermission());
      }
    } catch (IOException ioe) {
      log.warn("Failed to set permission for directory " + path, ioe);
    }

    String owner = Strings.isNullOrEmpty(ownerAndPermission.getOwner()) ? null : ownerAndPermission.getOwner();
    String group = Strings.isNullOrEmpty(ownerAndPermission.getGroup()) ? null : ownerAndPermission.getGroup();

    try {
      if (owner != null || group != null) {
        this.fs.setOwner(path, owner, group);
      }
    } catch (IOException ioe) {
      log.warn("Failed to set owner and/or group for path " + path, ioe);
    }

  }

  /**
   * Sets the {@link FsPermission}, owner, group for the path passed. And recursively to all directories and files under
   * it.
   */
  private void setRecursivePermission(Path path, OwnerAndPermission ownerAndPermission) throws IOException {
    List<FileStatus> files = FileListUtils.listPathsRecursively(fs, path, FileListUtils.NO_OP_PATH_FILTER);

    // Set permissions bottom up. Permissions are set to files first and then directories
    Collections.reverse(files);

    for (FileStatus file : files) {
      safeSetPathPermission(file.getPath(), addExecutePermissionsIfRequired(file, ownerAndPermission));
    }
  }

  /**
   * The method makes sure it always grants execute permissions for an owner if the <code>file</code> passed is a
   * directory. The publisher needs it to publish it to the final directory and list files under this directory.
   */
  private OwnerAndPermission addExecutePermissionsIfRequired(FileStatus file, OwnerAndPermission ownerAndPermission) {

    if (ownerAndPermission.getFsPermission() == null) {
      return ownerAndPermission;
    }

    if (!file.isDir()) {
      return ownerAndPermission;
    }
    FsAction newOwnerAction = ownerAndPermission.getFsPermission().getUserAction();

    switch (ownerAndPermission.getFsPermission().getUserAction()) {
      case READ:
        newOwnerAction = FsAction.READ_EXECUTE;
        break;
      case WRITE:
        newOwnerAction = FsAction.WRITE_EXECUTE;
        break;
      case READ_WRITE:
        newOwnerAction = FsAction.ALL;
        break;
      default:
        break;
    }

    FsPermission withExecute =
        new FsPermission(newOwnerAction, ownerAndPermission.getFsPermission().getGroupAction(), ownerAndPermission
            .getFsPermission().getOtherAction());

    return new OwnerAndPermission(ownerAndPermission.getOwner(), ownerAndPermission.getGroup(), withExecute);

  }

  @Override
  public long recordsWritten() {
    return this.filesWritten.get();
  }

  @Override
  public long bytesWritten() throws IOException {
    return this.bytesWritten.get();
  }

  @Override
  public void close() throws IOException {
    this.closer.close();
  }

  /**
   * Moves the file from task staging to task output. Each task has its own staging directory but all the tasks share
   * the same task output directory.
   *
   * {@inheritDoc}
   *
   * @see gobblin.writer.DataWriter#commit()
   */
  @Override
  public void commit() throws IOException {
    log.info(String.format("Committing data from %s to %s", this.stagingDir, this.outputDir));
    HadoopUtils.renameRecursively(this.fs, this.stagingDir, this.outputDir);
    this.fs.delete(this.stagingDir, true);
  }

  @Override
  public void cleanup() throws IOException {
    // Do nothing
  }
}
