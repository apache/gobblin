package gobblin.data.management.dataset;

import gobblin.data.management.retention.profile.ConfigurableGlobDatasetFinder;
import gobblin.dataset.FileSystemDataset;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.IOException;
import java.util.Properties;

/**
 * A subclass of {@link ConfigurableGlobDatasetFinder} which find all the {@link FileSystemDataset}
 * that matches a given glob pattern.
 */
public class DefaultFileSystemGlobFinder extends ConfigurableGlobDatasetFinder<FileSystemDataset> {
  public DefaultFileSystemGlobFinder(FileSystem fs, Properties properties) throws IOException {
    super(fs, properties);
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
