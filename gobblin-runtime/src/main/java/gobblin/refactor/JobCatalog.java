package gobblin.refactor;

import java.net.URI;
import java.util.Collection;


public interface JobCatalog {

  /** Returns a {@link Collection} of {@link JobSpec}s that are known to the catalog. */
  Collection<JobSpec> getJobs();

  /** Get a {@link JobSpec} by uri. */
  JobSpec getJob(URI uri);

  /**
   * Adds a {@link JobCatalogListener} that will be invoked upon updates on the
   * {@link JobCatalog}.
   */
  void addListener(JobCatalogListener jobListener);
}
