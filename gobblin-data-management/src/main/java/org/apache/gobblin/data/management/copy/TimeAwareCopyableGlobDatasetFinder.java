package org.apache.gobblin.data.management.copy;

import java.io.IOException;
import java.util.Properties;
import org.apache.gobblin.data.management.retention.profile.ConfigurableGlobDatasetFinder;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


/**
 * {@link org.apache.gobblin.data.management.retention.profile.ConfigurableGlobDatasetFinder} that returns datasets of type
 * {@link org.apache.gobblin.data.management.copy.TimeAwareRecursiveCopyableDataset}.N
 */
public class TimeAwareCopyableGlobDatasetFinder extends ConfigurableGlobDatasetFinder<CopyableDataset> {

  public TimeAwareCopyableGlobDatasetFinder(FileSystem fs, Properties props) {
    super(fs, props);
  }

  @Override
  public CopyableDataset datasetAtPath(Path path) throws IOException {
    return new TimeAwareRecursiveCopyableDataset(this.fs,path,this.props,this.datasetPattern);
  }
}
