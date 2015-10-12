package gobblin.data.management.retention.version.finder;

import java.io.IOException;
import java.util.Collection;
import java.util.Properties;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.joda.time.DateTime;

import com.google.common.collect.Lists;

import gobblin.data.management.retention.dataset.Dataset;
import gobblin.data.management.retention.version.DatasetVersion;
import gobblin.data.management.retention.version.TimestampedDatasetVersion;


/**
 * {@link VersionFinder} for datasets based on modification timestamps.
 */
public class ModDateTimeDatasetVersionFinder implements VersionFinder<TimestampedDatasetVersion> {

  private final FileSystem fs;

  public ModDateTimeDatasetVersionFinder(FileSystem fs, Properties props) {
    this.fs = fs;
  }

  @Override
  public Class<? extends DatasetVersion> versionClass() {
    return TimestampedDatasetVersion.class;
  }

  @Override
  public Collection<TimestampedDatasetVersion> findDatasetVersions(Dataset dataset) throws IOException {
    FileStatus status = this.fs.getFileStatus(dataset.datasetRoot());
    return Lists
        .newArrayList(new TimestampedDatasetVersion(new DateTime(status.getModificationTime()), dataset.datasetRoot()));
  }
}
