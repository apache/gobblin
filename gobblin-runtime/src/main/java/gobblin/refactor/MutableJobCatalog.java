package gobblin.refactor;

import java.net.URI;
import java.util.Collection;


/**
 *  A {@link JobCatalog} that can have its {@link Collection} of {@link JobSpec}s modified.
 *  */
public interface MutableJobCatalog extends JobCatalog {
  void put(JobSpec jobSpec);
  void remove(URI uri);
}
