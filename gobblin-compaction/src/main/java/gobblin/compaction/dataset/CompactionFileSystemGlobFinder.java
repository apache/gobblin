package gobblin.compaction.dataset;

import gobblin.configuration.State;
import gobblin.data.management.retention.profile.ConfigurableGlobDatasetFinder;
import gobblin.dataset.FileSystemDataset;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.IOException;

/**
 * A subclass of {@link ConfigurableGlobDatasetFinder} which find all the {@link FileSystemDataset}
 * that matches a given glob pattern.
 */
public class CompactionFileSystemGlobFinder extends ConfigurableGlobDatasetFinder<FileSystemDataset> {
  public CompactionFileSystemGlobFinder(FileSystem fs, State state) throws IOException {
    super(fs, state.getProperties());
  }

  public FileSystemDataset datasetAtPath(final Path path) throws IOException {
    return new FileSystemDataset() {
      @Override
      public Path datasetRoot() {
        return path;
      }

      @Override
      public String datasetURN() {
        return path.toString();
      }
    };
  }
}
