package gobblin.data.management.retention.policy;

import java.util.List;

import gobblin.data.management.retention.version.DatasetVersion;


/**
 * Retention policy around versions of a dataset. Specifies which versions of a dataset should be deleted by
 * {@link gobblin.data.management.retention.DatasetCleaner}.
 * @param <T> {@link gobblin.data.management.retention.version.DatasetVersion} accepted by this policy.
 */
public interface RetentionPolicy<T extends DatasetVersion> {

  /**
   * Should return class of T.
   * @return class of T.
   */
  public Class<? extends DatasetVersion> versionClass();

  /**
   * Logic to decide which dataset versions should be deleted. Only datasets returned will be deleted from filesystem.
   *
   * @param allVersions {@link java.util.SortedMap} of all dataset versions in the file system,
   *                                               sorted from newest to oldest.
   * @return Map of dataset versions that should be deleted.
   */
  public List<T> preserveDeletableVersions(List<T> allVersions);

}
