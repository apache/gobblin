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

package gobblin.data.management.copy.recovery;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;

import gobblin.configuration.State;
import gobblin.data.management.copy.CopySource;
import gobblin.data.management.copy.CopyableFile;
import gobblin.util.guid.Guid;


/**
 * Helper class for distcp work unit recovery.
 */
@Slf4j
public class RecoveryHelper {

  public static final String PERSIST_DIR_KEY = "distcp.persist.dir";

  private final FileSystem fs;
  private final Optional<Path> persistDir;

  public RecoveryHelper(FileSystem fs, State state) throws IOException {
    this.fs = fs;
    this.persistDir = getPersistDir(state);
  }

  /**
   * Get the persist directory for this job.
   * @param state {@link State} containing job information.
   * @return A {@link Path} used as persist directory for this job. Note this path is user-specific for security reasons.
   * @throws java.io.IOException
   */
  public static Optional<Path> getPersistDir(State state) throws IOException {
    if (state.contains(PERSIST_DIR_KEY)) {
      return Optional.of(new Path(state.getProp(PERSIST_DIR_KEY),
          UserGroupInformation.getCurrentUser().getShortUserName()));
    } else {
      return Optional.absent();
    }
  }

  /**
   * Moves a copied path into a persistent location managed by gobblin-distcp. This method is used when an already
   * copied file cannot be successfully published. In future runs, instead of re-copying the file, distcp will use the
   * persisted file.
   *
   * @param state {@link State} containing job information.
   * @param file {@link CopyableFile} from which input {@link Path} originated.
   * @param path {@link Path} to persist.
   * @return true if persist was successful.
   * @throws IOException
   */
  public boolean persistFile(State state, CopyableFile file, Path path) throws IOException {

    if (!this.persistDir.isPresent()) {
      return false;
    }

    String guid = computeGuid(state, file);
    StringBuilder nameBuilder = new StringBuilder(guid);
    nameBuilder.append("_");
    nameBuilder.append(shortenPathName(file.getOrigin().getPath(), 250 - nameBuilder.length()));

    if (!this.fs.exists(this.persistDir.get())) {
      this.fs.mkdirs(this.persistDir.get(), new FsPermission(FsAction.ALL, FsAction.READ, FsAction.NONE));
    }

    Path targetPath = new Path(persistDir.get(), nameBuilder.toString());
    log.info(String.format("Persisting file %s with guid %s to location %s.", path, guid, targetPath));
    return this.fs.rename(path, targetPath);
  }

  /**
   * Searches the persist directory to find {@link Path}s matching the input {@link CopyableFile}.
   * @param state {@link State} containing job information.
   * @param file {@link CopyableFile} for which persisted {@link Path}s should be found.
   * @param filter {@link com.google.common.base.Predicate} used to filter found paths.
   * @return Optionally, a {@link Path} in the {@link FileSystem} that is the desired copy of the {@link CopyableFile}.
   * @throws IOException
   */
  public Optional<FileStatus> findPersistedFile(State state, CopyableFile file, Predicate<FileStatus> filter)
      throws IOException {
    if (!this.persistDir.isPresent() || !this.fs.exists(this.persistDir.get())) {
      return Optional.absent();
    }

    Path glob = new Path(this.persistDir.get(), computeGuid(state, file) + "_*");
    for (FileStatus fileStatus : this.fs.globStatus(glob)) {
      if (filter.apply(fileStatus)) {
        return Optional.of(fileStatus);
      }
    }
    return Optional.absent();
  }

  /**
   * Shorten an absolute path into a sanitized String of length at most bytes. This is useful for including a summary
   * of an absolute path in a file name.
   *
   * <p>
   *   For example: shortenPathName("/user/gobblin/foo/bar/myFile.txt", 25) will be shortened to "_user_gobbl..._myFile.txt".
   * </p>
   *
   * @param path absolute {@link Path} to shorten.
   * @param bytes max number of UTF8 bytes that output string can use (note that,
   *              for now, it is assumed that each character uses exactly one byte).
   * @return a shortened, sanitized String of length at most bytes.
   */
  static String shortenPathName(Path path, int bytes) {
    String pathString = path.toUri().getPath();
    String replaced = pathString.replace("/", "_");

    if (replaced.length() <= bytes) {
      return replaced;
    }

    int bytesPerHalf = (bytes - 3) / 2;
    return replaced.substring(0, bytesPerHalf) + "..." + replaced.substring(replaced.length() - bytesPerHalf);
  }

  private static String computeGuid(State state, CopyableFile file) throws IOException {
    Optional<Guid> stateGuid = CopySource.getWorkUnitGuid(state);
    if (stateGuid.isPresent()) {
      return Guid.combine(file.guid(), stateGuid.get()).toString();
    } else {
      throw new IOException("State does not contain a guid.");
    }
  }
}
