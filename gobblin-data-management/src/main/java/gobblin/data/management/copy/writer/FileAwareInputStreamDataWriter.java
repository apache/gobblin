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

package gobblin.data.management.copy.writer;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.data.management.copy.CopyableFile;
import gobblin.data.management.copy.FileAwareInputStream;
import gobblin.data.management.copy.OwnerAndPermission;
import gobblin.data.management.util.PathUtils;
import gobblin.util.FileListUtils;
import gobblin.util.ForkOperatorUtils;
import gobblin.util.HadoopUtils;
import gobblin.util.WriterUtils;
import gobblin.util.io.StreamUtils;
import gobblin.writer.DataWriter;

import java.io.IOException;
import java.net.URI;
import java.util.List;

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;

import com.google.common.io.Closer;


/**
 * A {@link DataWriter} to write {@link FileAwareInputStream}
 */
@Slf4j
public class FileAwareInputStreamDataWriter implements DataWriter<FileAwareInputStream> {

  protected long bytesWritten = 0;
  protected long filesWritten = 0;
  protected final State state;
  protected final FileSystem fs;
  protected final Path stagingDir;
  protected final Path outputDir;
  protected Closer closer = Closer.create();

  public FileAwareInputStreamDataWriter(State state, int numBranches, int branchId) throws IOException {
    this.state = state;

    Configuration conf = new Configuration();
    String uri =
        this.state
            .getProp(ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_FILE_SYSTEM_URI, numBranches,
                branchId), ConfigurationKeys.LOCAL_FS_URI);

    this.fs = FileSystem.get(URI.create(uri), conf);
    this.stagingDir = WriterUtils.getWriterStagingDir(state, numBranches, branchId);
    this.outputDir =
        new Path(state.getProp(ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_OUTPUT_DIR,
            numBranches, branchId)));
  }

  @Override
  public void write(FileAwareInputStream fileAwareInputStream) throws IOException {
    fileAwareInputStream.getInputStream();
    Path stagingFile = getStagingFilePath(fileAwareInputStream.getFile());
    this.fs.mkdirs(stagingFile.getParent(), fileAwareInputStream.getFile().getDestinationOwnerAndPermission()
        .getFsPermission());

    FSDataOutputStream os = fs.create(stagingFile, true);
    try {
      IOUtils.copyBytes(fileAwareInputStream.getInputStream(), os, fs.getConf(), false);
    } finally {
      os.close();
      fileAwareInputStream.getInputStream().close();
    }

    filesWritten++;

    setFilePermissions(fileAwareInputStream.getFile());
  }

  /**
   * Sets the owner/group and permission for the file in the task staging directory
   */
  protected void setFilePermissions(CopyableFile file) {
    try {
      setAncestorPermissions(file);
      setRecursivePermission(getStagingFilePath(file), file.getDestinationOwnerAndPermission());
    } catch (IOException e) {
      log.error("Failed to set permissions for " + file.getOrigin(), e);
    }

  }

  protected Path getStagingFilePath(CopyableFile file) {
    return new Path(this.stagingDir, PathUtils.withoutLeadingSeparator(file.getDestination()));
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
    Path parentPath = file.getDestination().getParent();
    for (OwnerAndPermission ownerAndPermission : file.getAncestorsOwnerAndPermission()) {
      if (parentPath == null) {
        log.info("Ancestor owner and permission may not be set correctly. Exhausted parent paths before ancestor permissions");
        log.info(String.format("File destination path %s, AncestorOwnerAndPermissions size %s.", file.getDestination(),
            file.getAncestorsOwnerAndPermission().size()));
        break;
      }
      setPathPermission(parentPath, ownerAndPermission);
      parentPath = parentPath.getParent();
    }
  }

  /**
   * Sets the {@link FsPermission}, owner, group for the path passed.
   */
  private void setPathPermission(Path path, OwnerAndPermission ownerAndPermission) throws IOException {
    fs.setPermission(path, ownerAndPermission.getFsPermission());

    if (StringUtils.isNotBlank(ownerAndPermission.getGroup()) && StringUtils.isNotBlank(ownerAndPermission.getOwner())) {
      fs.setOwner(path, ownerAndPermission.getOwner(), ownerAndPermission.getGroup());
    } else {
      log.info("Owner and group will not be set as no valid user and group available for " + path);
    }
  }

  /**
   * Sets the {@link FsPermission}, owner, group for the path passed. And recursively to all directories and files under
   * it.
   */
  private void setRecursivePermission(Path path, OwnerAndPermission ownerAndPermission) throws IOException {
    List<FileStatus> files = FileListUtils.listPathsRecursively(fs, path, FileListUtils.NO_OP_PATH_FILTER);

    for (FileStatus file : files) {
      setPathPermission(file.getPath(), ownerAndPermission);
    }
  }

  @Override
  public long recordsWritten() {
    return filesWritten;
  }

  @Override
  public long bytesWritten() throws IOException {
    return bytesWritten;
  }

  @Override
  public void close() throws IOException {
    closer.close();
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
    HadoopUtils.safeRenameRecursively(fs, stagingDir, outputDir);
  }

  @Override
  public void cleanup() throws IOException {
  }
}
