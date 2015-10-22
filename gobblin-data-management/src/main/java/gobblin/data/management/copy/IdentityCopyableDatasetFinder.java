package gobblin.data.management.copy;

import gobblin.data.management.retention.dataset.finder.DatasetFinder;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import lombok.AllArgsConstructor;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.ImmutableList;


/**
 * An implementation of {@link DatasetFinder} that create a single {@link CopyableDataset} for the root path provided
 */
@AllArgsConstructor
public class IdentityCopyableDatasetFinder implements DatasetFinder<CopyableDataset> {

  private Path datasetRootPath;
  private FileSystem fs;
  private Properties properties;

  @Override
  public List<CopyableDataset> findDatasets() throws IOException {
    CopyableDataset dataset = new RecursiveCopyableDataset(fs, datasetRootPath, properties);
    return ImmutableList.of(dataset);
  }
}
