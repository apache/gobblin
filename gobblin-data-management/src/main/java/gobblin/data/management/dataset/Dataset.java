package gobblin.data.management.dataset;

import org.apache.hadoop.fs.Path;


/**
 * Interface representing a dataset.
 */
public interface Dataset {
  /**
   * Deepest {@link org.apache.hadoop.fs.Path} that contains all files in the dataset.
   */
  public Path datasetRoot();

}
