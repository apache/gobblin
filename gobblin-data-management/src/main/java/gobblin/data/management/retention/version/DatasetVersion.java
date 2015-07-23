package gobblin.data.management.retention.version;

import java.util.Set;

import org.apache.hadoop.fs.Path;


/**
 * Wrapper around {@link java.lang.Comparable} for dataset versions.
 */
public interface DatasetVersion extends Comparable<DatasetVersion> {

  /**
   * Get set of {@link org.apache.hadoop.fs.Path} that should be deleted to delete this dataset version.
   *
   * <p>
   *   Each path will be deleted recursively, and the deletions will be done serially. As such, this set should be
   *   the minimal set of {@link org.apache.hadoop.fs.Path} that can be deleted to remove the dataset version.
   *   (For example, the parent directory of the files in the dataset, assuming all descendants of that
   *   directory are files for this dataset version).
   * </p>
   *
   * @return Minimal set of {@link org.apache.hadoop.fs.Path} to delete in order to remove the dataset version.
   */
  public Set<Path> getPathsToDelete();

}
