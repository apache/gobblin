package gobblin.data.management.copy;

import gobblin.data.management.dataset.Dataset;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;


/**
 * {@link Dataset} that supports finding {@link CopyableFile}s.
 */
public interface CopyableDataset extends Dataset {

  /**
   * Find all {@link CopyableFile}s in this dataset.
   * @return List of {@link CopyableFile}s in this dataset.
   * @throws IOException
   * @param targetFileSystem
   */
  public List<CopyableFile> getCopyableFiles(FileSystem targetFileSystem) throws IOException;

}
