package gobblin.data.management.copy.prioritization;

import java.util.Comparator;

import com.typesafe.config.Config;

import gobblin.data.management.copy.CopyEntity;
import gobblin.data.management.partition.FileSet;


/**
 * An alias for a {@link Comparator} of {@link FileSet} for type safety.
 */
public interface FileSetComparator extends Comparator<FileSet<CopyEntity>> {

  public interface Factory {
    FileSetComparator create(Config config);
  }

}
