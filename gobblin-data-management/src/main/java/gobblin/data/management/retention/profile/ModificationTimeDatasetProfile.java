package gobblin.data.management.retention.profile;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import gobblin.data.management.retention.dataset.Dataset;
import gobblin.data.management.retention.dataset.ModificationTimeDataset;


/**
 * {@link gobblin.data.management.retention.dataset.finder.DatasetFinder} for {@link ModificationTimeDataset}s.
 *
 * Modification time datasets will be cleaned by the modification timestamps of the datasets that match
 * 'gobblin.retention.dataset.pattern'.
 */
public class ModificationTimeDatasetProfile extends ConfigurableGlobDatasetFinder {
  public ModificationTimeDatasetProfile(FileSystem fs, Properties props) throws IOException {
    super(fs, props);
  }

  @Override
  public Dataset datasetAtPath(Path path) throws IOException {
    return new ModificationTimeDataset(this.fs, this.props, path);
  }
}
