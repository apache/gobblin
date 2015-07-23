package gobblin.data.management.trash;

import org.apache.hadoop.fs.FileStatus;

/**
 * Policy for determining whether a {@link gobblin.data.management.trash.Trash} snapshot should be deleted.
 */
public interface SnapshotCleanupPolicy {
  /**
   * Decide whether a trash snapshot should be permanently deleted from the file system.
   *
   * <p>
   *   This method will be called for all snapshots in the trash directory, in order from oldest to newest.
   * </p>
   *
   * @param snapshot {@link org.apache.hadoop.fs.FileStatus} of candidate snapshot for deletion.
   * @param trash {@link gobblin.data.management.trash.Trash} object that called this method.
   * @return true if the snapshot should be deleted permanently.
   */
  boolean shouldDeleteSnapshot(FileStatus snapshot, Trash trash);
}
