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

package gobblin.util.commit;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import javax.annotation.Nullable;
import lombok.Getter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import gobblin.commit.CommitStep;
import gobblin.data.management.trash.Trash;
import gobblin.data.management.trash.TrashFactory;
import gobblin.util.PathUtils;


/**
 * {@link CommitStep} to delete a set of paths in a {@link FileSystem}.
 * If {@link #parentDeletionLimit} is present, will also delete newly empty parent directories up to but not including
 * that limit.
 */
@Getter
public class DeleteFileCommitStep implements CommitStep {

  private final Collection<FileStatus> pathsToDelete;
  private final Properties properties;
  private final URI fsUri;
  private final Optional<Path> parentDeletionLimit;

  public DeleteFileCommitStep(FileSystem fs, Path path, Properties properties) throws IOException {
    this(fs, Lists.newArrayList(fs.getFileStatus(path)), properties, Optional.<Path>absent());
  }

  public static DeleteFileCommitStep fromPaths(FileSystem fs, Collection<Path> paths, Properties properties) throws
      IOException {
    return new DeleteFileCommitStep(fs, toFileStatus(fs, paths), properties, Optional.<Path>absent());
  }

  public static DeleteFileCommitStep fromPaths(FileSystem fs, Collection<Path> paths, Properties properties,
      Path parentDeletionLimit) throws IOException {
    return new DeleteFileCommitStep(fs, toFileStatus(fs, paths), properties, Optional.of(parentDeletionLimit));
  }

  /**
   * @param fs {@link FileSystem} where files need to be deleted.
   * @param paths Collection of {@link FileStatus}es to deleted.
   * @param properties {@link Properties} object including {@link Trash} configuration.
   * @param parentDeletionLimit if present, will delete empty parent directories up to but not including this path. If
   *                            absent, will not delete empty parent directories.
   * @throws IOException
   */
  public DeleteFileCommitStep(FileSystem fs, Collection<FileStatus> paths, Properties properties,
      Optional<Path> parentDeletionLimit) throws IOException {
    this.fsUri = fs.getUri();
    this.pathsToDelete = paths;
    this.properties = properties;
    this.parentDeletionLimit = parentDeletionLimit;
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
    Set<Path> parents = Sets.newHashSet();
    for (FileStatus pathToDelete : this.pathsToDelete) {
      if (existsAndIsExpectedFile(pathToDelete)) {
        trash.moveToTrash(pathToDelete.getPath());
        parents.add(pathToDelete.getPath().getParent());
      }
    }
    if (this.parentDeletionLimit.isPresent()) {
      for (Path parent : parents) {
        PathUtils.deleteEmptyParentDirectories(getFS(), this.parentDeletionLimit.get(), parent);
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

  @Override
  public String toString() {
    return String.format("Delete the following files at %s: %s", this.fsUri,
        Iterables.toString(Iterables.transform(this.pathsToDelete, new Function<FileStatus, Path>() {
      @Nullable
      @Override
      public Path apply(@Nullable FileStatus input) {
        return input != null ? input.getPath() : null;
      }
    })));
  }
}
