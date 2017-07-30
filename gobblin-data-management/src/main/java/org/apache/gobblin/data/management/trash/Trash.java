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

package gobblin.data.management.trash;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gobblin.util.PathUtils;


/**
 * Flexible implementation of Trash similar to Hadoop trash. Allows for injecting cleanup policies for snapshots.
 */
public class Trash implements GobblinTrash {

  private static final Logger LOG = LoggerFactory.getLogger(Trash.class);
  private static final FsPermission PERM = new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE);
  private static final FsPermission ALL_PERM = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL);

  /**
   * Location of trash directory in file system. The location can include a token $USER that will be automatically
   * replaced by the name of the active user.
   */
  public static final String TRASH_LOCATION_KEY = "gobblin.trash.location";
  public static final String SNAPSHOT_CLEANUP_POLICY_CLASS_KEY = "gobblin.trash.snapshot.cleanup.policy.class";
  public static final String TRASH_SNAPSHOT_PREFIX = "_TRASH_SNAPSHOT_";
  public static final String TRASH_IDENTIFIER_FILE = "_THIS_IS_TRASH_DIRECTORY";
  public static final String DEFAULT_TRASH_DIRECTORY = "_GOBBLIN_TRASH";
  public static final DateTimeFormatter TRASH_SNAPSHOT_NAME_FORMATTER =
      DateTimeFormat.forPattern(String.format("'%s'yyyyMMddHHmmss", TRASH_SNAPSHOT_PREFIX)).withZone(DateTimeZone.UTC);
  public static final PathFilter TRASH_SNAPSHOT_PATH_FILTER = new PathFilter() {
    @Override
    public boolean accept(Path path) {
      return !path.getName().equals(TRASH_IDENTIFIER_FILE) && path.getName().startsWith(TRASH_SNAPSHOT_PREFIX);
    }
  };
  public static final PathFilter TRASH_NOT_SNAPSHOT_PATH_FILTER = new PathFilter() {
    @Override
    public boolean accept(Path path) {
      return !path.getName().equals(TRASH_IDENTIFIER_FILE) && !path.getName().startsWith(TRASH_SNAPSHOT_PREFIX);
    }
  };

  /**
   * Get trash location.
   * @return {@link org.apache.hadoop.fs.Path} for trash directory.
   * @throws IOException
   */
  public Path getTrashLocation() throws IOException {
    return this.trashLocation;
  }

  /**
   * Create location of Trash directory. Parsed from props at key {@link #TRASH_LOCATION_KEY}, defaulting to
   * /home/directory/_GOBBLIN_TRASH.
   * @param fs {@link org.apache.hadoop.fs.FileSystem} where trash should be found.
   * @param props {@link java.util.Properties} containing trash configuration.
   * @param user If the trash location contains the token $USER, the token will be replaced by the value of user.
   * @return {@link org.apache.hadoop.fs.Path} for trash directory.
   * @throws java.io.IOException
   */
  protected Path createTrashLocation(FileSystem fs, Properties props, String user) throws IOException {
    Path trashLocation;
    if (props.containsKey(TRASH_LOCATION_KEY)) {
      trashLocation = new Path(props.getProperty(TRASH_LOCATION_KEY).replaceAll("\\$USER", user));
    } else {
      trashLocation = new Path(fs.getHomeDirectory(), DEFAULT_TRASH_DIRECTORY);
      LOG.info("Using default trash location at " + trashLocation);
    }
    if (!trashLocation.isAbsolute()) {
      throw new IllegalArgumentException("Trash location must be absolute. Found " + trashLocation.toString());
    }
    Path qualifiedTrashLocation = fs.makeQualified(trashLocation);
    ensureTrashLocationExists(fs, qualifiedTrashLocation);
    return qualifiedTrashLocation;
  }

  protected void ensureTrashLocationExists(FileSystem fs, Path trashLocation) throws IOException {
    if (fs.exists(trashLocation)) {
      if (!fs.isDirectory(trashLocation)) {
        throw new IOException(String.format("Trash location %s is not a directory.", trashLocation));
      }

      if (!fs.exists(new Path(trashLocation, TRASH_IDENTIFIER_FILE))) {
        // If trash identifier file is not present, directory might have been created by user.
        // Add trash identifier file only if directory is empty.
        if (fs.listStatus(trashLocation).length > 0) {
          throw new IOException(String.format("Trash directory %s exists, but it does not look like a trash directory. "
              + "File: %s missing and directory is not empty.", trashLocation, TRASH_IDENTIFIER_FILE));
        } else if (!fs.createNewFile(new Path(trashLocation, TRASH_IDENTIFIER_FILE))) {
          throw new IOException(String.format("Failed to create file %s in existing trash directory %s.",
              TRASH_IDENTIFIER_FILE, trashLocation));
        }
      }
    } else if (!(safeFsMkdir(fs, trashLocation.getParent(), ALL_PERM) && safeFsMkdir(fs, trashLocation, PERM)
        && fs.createNewFile(new Path(trashLocation, TRASH_IDENTIFIER_FILE)))) {
      // Failed to create directory or create trash identifier file.
      throw new IOException("Failed to create trash directory at " + trashLocation.toString());
    }
  }

  protected final FileSystem fs;
  private final Path trashLocation;
  private final SnapshotCleanupPolicy snapshotCleanupPolicy;

  /**
   * @deprecated Use {@link gobblin.data.management.trash.TrashFactory}.
   */
  @Deprecated
  public Trash(FileSystem fs) throws IOException {
    this(fs, new Properties());
  }

  /**
   * @deprecated Use {@link gobblin.data.management.trash.TrashFactory}.
   */
  @Deprecated
  public Trash(FileSystem fs, Properties props) throws IOException {
    this(fs, props, UserGroupInformation.getCurrentUser().getUserName());
  }

  protected Trash(FileSystem fs, Properties props, String user) throws IOException {
    this.fs = fs;
    this.trashLocation = createTrashLocation(fs, props, user);
    try {
      Class<?> snapshotCleanupPolicyClass = Class.forName(props.getProperty(SNAPSHOT_CLEANUP_POLICY_CLASS_KEY,
          TimeBasedSnapshotCleanupPolicy.class.getCanonicalName()));
      this.snapshotCleanupPolicy =
          (SnapshotCleanupPolicy) snapshotCleanupPolicyClass.getConstructor(Properties.class).newInstance(props);
    } catch (Exception exception) {
      throw new IllegalArgumentException("Could not create snapshot cleanup policy with class " + props
          .getProperty(SNAPSHOT_CLEANUP_POLICY_CLASS_KEY, TimeBasedSnapshotCleanupPolicy.class.getCanonicalName()),
          exception);
    }
  }

  /**
   * Move a path to trash. The absolute path of the input path will be replicated under the trash directory.
   * @param path {@link org.apache.hadoop.fs.FileSystem} path to move to trash.
   * @return true if move to trash was done successfully.
   * @throws IOException
   */
  @Override
  public boolean moveToTrash(Path path) throws IOException {
    Path fullyResolvedPath = path.isAbsolute() ? path : new Path(this.fs.getWorkingDirectory(), path);
    Path targetPathInTrash = PathUtils.mergePaths(this.trashLocation, fullyResolvedPath);

    if (!this.fs.exists(targetPathInTrash.getParent())) {
      this.fs.mkdirs(targetPathInTrash.getParent());
    } else if (this.fs.exists(targetPathInTrash)) {
      targetPathInTrash = targetPathInTrash.suffix("_" + System.currentTimeMillis());
    }

    return this.fs.rename(fullyResolvedPath, targetPathInTrash);
  }

  /**
   * Moves all current contents of trash directory into a snapshot directory with current timestamp.
   * @throws IOException
   */
  public void createTrashSnapshot() throws IOException {
    FileStatus[] pathsInTrash = this.fs.listStatus(this.trashLocation, TRASH_NOT_SNAPSHOT_PATH_FILTER);

    if (pathsInTrash.length <= 0) {
      LOG.info("Nothing in trash. Will not create snapshot.");
      return;
    }

    Path snapshotDir = new Path(this.trashLocation, new DateTime().toString(TRASH_SNAPSHOT_NAME_FORMATTER));
    if (this.fs.exists(snapshotDir)) {
      throw new IOException("New snapshot directory " + snapshotDir.toString() + " already exists.");
    }

    if (!safeFsMkdir(fs, snapshotDir, PERM)) {
      throw new IOException("Failed to create new snapshot directory at " + snapshotDir.toString());
    }

    LOG.info(String.format("Moving %d paths in Trash directory to newly created snapshot at %s.", pathsInTrash.length,
        snapshotDir.toString()));

    int pathsFailedToMove = 0;
    for (FileStatus fileStatus : pathsInTrash) {
      Path pathRelativeToTrash = PathUtils.relativizePath(fileStatus.getPath(), this.trashLocation);
      Path targetPath = new Path(snapshotDir, pathRelativeToTrash);
      boolean movedThisPath = true;
      try {
        movedThisPath = this.fs.rename(fileStatus.getPath(), targetPath);
      } catch (IOException exception) {
        LOG.error("Failed to move path " + fileStatus.getPath().toString() + " to snapshot.", exception);
        pathsFailedToMove += 1;
        continue;
      }
      if (!movedThisPath) {
        LOG.error("Failed to move path " + fileStatus.getPath().toString() + " to snapshot.");
        pathsFailedToMove += 1;
      }
    }

    if (pathsFailedToMove > 0) {
      LOG.error(
          String.format("Failed to move %d paths to the snapshot at %s.", pathsFailedToMove, snapshotDir.toString()));
    }

  }

  /**
   * For each existing trash snapshot, uses a {@link gobblin.data.management.trash.SnapshotCleanupPolicy} to determine whether
   * the snapshot should be deleted. If so, delete it permanently.
   *
   * <p>
   *   Each existing snapshot will be passed to {@link gobblin.data.management.trash.SnapshotCleanupPolicy#shouldDeleteSnapshot}
   *   from oldest to newest, and will be deleted if the method returns true.
   * </p>
   *
   * @throws IOException
   */
  public void purgeTrashSnapshots() throws IOException {
    List<FileStatus> snapshotsInTrash =
        Arrays.asList(this.fs.listStatus(this.trashLocation, TRASH_SNAPSHOT_PATH_FILTER));

    Collections.sort(snapshotsInTrash, new Comparator<FileStatus>() {
      @Override
      public int compare(FileStatus o1, FileStatus o2) {
        return TRASH_SNAPSHOT_NAME_FORMATTER.parseDateTime(o1.getPath().getName())
            .compareTo(TRASH_SNAPSHOT_NAME_FORMATTER.parseDateTime(o2.getPath().getName()));
      }
    });

    int totalSnapshots = snapshotsInTrash.size();
    int snapshotsDeleted = 0;

    for (FileStatus snapshot : snapshotsInTrash) {
      if (this.snapshotCleanupPolicy.shouldDeleteSnapshot(snapshot, this)) {
        try {
          boolean successfullyDeleted = this.fs.delete(snapshot.getPath(), true);
          if (successfullyDeleted) {
            snapshotsDeleted++;
          } else {
            LOG.error("Failed to delete snapshot " + snapshot.getPath());
          }
        } catch (IOException exception) {
          LOG.error("Failed to delete snapshot " + snapshot.getPath(), exception);
        }
      }
    }

    LOG.info(String.format("Deleted %d out of %d existing snapshots.", snapshotsDeleted, totalSnapshots));
  }

  /**
   * Safe creation of trash folder to ensure thread-safe.
   * @throws IOException
   */
  private boolean safeFsMkdir(FileSystem fs, Path f, FsPermission permission) throws IOException {
    try {
      return fs.mkdirs(f, permission);
    } catch (IOException e) {
      // To handle the case when trash folder is created by other threads
      // The case is rare and we don't put synchronized keywords for performance consideration.
      if (!fs.exists(f)) {
        throw new IOException("Failed to create trash folder while it is still not existed yet.");
      } else {
        LOG.debug("Target folder %s has been created by other threads.", f.toString());
        return true;
      }
    }
  }
}
