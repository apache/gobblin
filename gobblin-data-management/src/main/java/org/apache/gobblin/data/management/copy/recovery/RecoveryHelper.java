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

package org.apache.gobblin.data.management.copy.recovery;

import lombok.extern.slf4j.Slf4j;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.data.management.copy.CopySource;
import org.apache.gobblin.data.management.copy.CopyEntity;
import org.apache.gobblin.data.management.copy.CopyableFile;
import org.apache.gobblin.util.guid.Guid;


/**
 * Helper class for distcp work unit recovery.
 */
@Slf4j
public class RecoveryHelper {

  public static final String PERSIST_DIR_KEY = "distcp.persist.dir";
  public static final String PERSIST_RETENTION_KEY = "distcp.persist.retention.hours";
  public static final int DEFAULT_PERSIST_RETENTION = 24;

  private final FileSystem fs;
  private final Optional<Path> persistDir;
  private final int retentionHours;

  public RecoveryHelper(FileSystem fs, State state) throws IOException {
    this.fs = fs;
    this.persistDir = getPersistDir(state);
    this.retentionHours = state.getPropAsInt(PERSIST_RETENTION_KEY, DEFAULT_PERSIST_RETENTION);
  }

  /**
   * Get the persist directory for this job.
   * @param state {@link State} containing job information.
   * @return A {@link Path} used as persist directory for this job. Note this path is user-specific for security reasons.
   * @throws java.io.IOException
   */
  public static Optional<Path> getPersistDir(State state) throws IOException {
    if (state.contains(PERSIST_DIR_KEY)) {
      return Optional
          .of(new Path(state.getProp(PERSIST_DIR_KEY), UserGroupInformation.getCurrentUser().getShortUserName()));
    }
    return Optional.absent();
  }

  /**
   * Moves a copied path into a persistent location managed by gobblin-distcp. This method is used when an already
   * copied file cannot be successfully published. In future runs, instead of re-copying the file, distcp will use the
   * persisted file.
   *
   * @param state {@link State} containing job information.
   * @param file {@link org.apache.gobblin.data.management.copy.CopyEntity} from which input {@link Path} originated.
   * @param path {@link Path} to persist.
   * @return true if persist was successful.
   * @throws IOException
   */
  public boolean persistFile(State state, CopyableFile file, Path path) throws IOException {

    if (!this.persistDir.isPresent()) {
      return false;
    }

    String guid = computeGuid(state, file);
    Path guidPath = new Path(this.persistDir.get(), guid);

    if (!this.fs.exists(guidPath)) {
      this.fs.mkdirs(guidPath, new FsPermission(FsAction.ALL, FsAction.READ, FsAction.NONE));
    }

    Path targetPath = new Path(guidPath, shortenPathName(file.getOrigin().getPath(), 250 - guid.length()));
    log.info(String.format("Persisting file %s with guid %s to location %s.", path, guid, targetPath));
    if (this.fs.rename(path, targetPath)) {
      this.fs.setTimes(targetPath, System.currentTimeMillis(), -1);
      return true;
    }
    return false;
  }

  /**
   * Searches the persist directory to find {@link Path}s matching the input {@link org.apache.gobblin.data.management.copy.CopyEntity}.
   * @param state {@link State} containing job information.
   * @param file {@link org.apache.gobblin.data.management.copy.CopyEntity} for which persisted {@link Path}s should be found.
   * @param filter {@link com.google.common.base.Predicate} used to filter found paths.
   * @return Optionally, a {@link Path} in the {@link FileSystem} that is the desired copy of the {@link org.apache.gobblin.data.management.copy.CopyEntity}.
   * @throws IOException
   */
  public Optional<FileStatus> findPersistedFile(State state, CopyEntity file, Predicate<FileStatus> filter)
      throws IOException {
    if (!this.persistDir.isPresent() || !this.fs.exists(this.persistDir.get())) {
      return Optional.absent();
    }

    Path guidPath = new Path(this.persistDir.get(), computeGuid(state, file));
    FileStatus[] statuses;
    try {
      statuses = this.fs.listStatus(guidPath);
    } catch (FileNotFoundException e) {
      return Optional.absent();
    }

    for (FileStatus fileStatus : statuses) {
      if (filter.apply(fileStatus)) {
        return Optional.of(fileStatus);
      }
    }
    return Optional.absent();
  }

  /**
   * Delete all persisted files older than the number of hours set by {@link #PERSIST_RETENTION_KEY}.
   * @throws IOException
   */
  public void purgeOldPersistedFile() throws IOException {
    if (!this.persistDir.isPresent() || !this.fs.exists(this.persistDir.get())) {
      log.info("No persist directory to clean.");
      return;
    }

    long retentionMillis = TimeUnit.HOURS.toMillis(this.retentionHours);
    long now = System.currentTimeMillis();

    for (FileStatus fileStatus : this.fs.listStatus(this.persistDir.get())) {
      if (now - fileStatus.getModificationTime() > retentionMillis) {
        if (!this.fs.delete(fileStatus.getPath(), true)) {
          log.warn("Failed to delete path " + fileStatus.getPath());
        }
      }
    }
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

  private static String computeGuid(State state, CopyEntity file) throws IOException {
    Optional<Guid> stateGuid = CopySource.getWorkUnitGuid(state);
    if (stateGuid.isPresent()) {
      return Guid.combine(file.guid(), stateGuid.get()).toString();
    }
    throw new IOException("State does not contain a guid.");
  }
}
