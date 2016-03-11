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

package gobblin.util.commit;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

import gobblin.commit.CommitStep;
import gobblin.data.management.trash.Trash;
import gobblin.data.management.trash.TrashFactory;


/**
 * {@link CommitStep} to delete a set of paths in a {@link FileSystem}.
 */
public class DeleteFileCommitStep implements CommitStep {

  private final Collection<FileStatus> pathsToDelete;
  private final Properties properties;
  private final URI fsUri;

  public DeleteFileCommitStep(FileSystem fs, Path path, Properties properties) throws IOException {
    this(fs, Lists.newArrayList(fs.getFileStatus(path)), properties);
  }

  public static DeleteFileCommitStep fromPaths(FileSystem fs, Collection<Path> paths, Properties properties) throws
      IOException {
    return new DeleteFileCommitStep(fs, toFileStatus(fs, paths), properties);
  }

  public DeleteFileCommitStep(FileSystem fs, Collection<FileStatus> paths, Properties properties) throws IOException {
    this.fsUri = fs.getUri();
    this.pathsToDelete = paths;
    this.properties = properties;
  }

  private static List<FileStatus> toFileStatus(FileSystem fs, Collection<Path> paths) throws IOException {
    List<FileStatus> fileStatuses = Lists.newArrayList();
    for (Path path : paths) {
      fileStatuses.add(fs.getFileStatus(path));
    }
    return fileStatuses;
  }

  @Override public boolean isCompleted() throws IOException {
    for (FileStatus pathToDelete : this.pathsToDelete) {
      if (existsAndIsExpectedFile(pathToDelete)) {
        return false;
      }
    }
    return true;
  }

  @Override public void execute() throws IOException {
    Trash trash = TrashFactory.createTrash(getFS(), this.properties);
    for (FileStatus pathToDelete : this.pathsToDelete) {
      if (existsAndIsExpectedFile(pathToDelete)) {
        trash.moveToTrash(pathToDelete.getPath());
      }
    }
  }

  /**
   * Checks whether existing file in filesystem is the expected file (compares length and modificaiton time).
   */
  private boolean existsAndIsExpectedFile(FileStatus status) throws IOException {

    if (!getFS().exists(status.getPath())) {
      return false;
    }

    FileStatus currentFileStatus = getFS().getFileStatus(status.getPath());

    if (currentFileStatus.getLen() != status.getLen() ||
        currentFileStatus.getModificationTime() > status.getModificationTime()) {
      return false;
    }

    return true;
  }

  private FileSystem getFS() throws IOException {
    return FileSystem.get(this.fsUri, new Configuration());
  }
}
