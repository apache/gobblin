package gobblin.data.management.retention.dataset;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gobblin.data.management.retention.policy.RetentionPolicy;
import gobblin.data.management.retention.policy.TimeBasedRetentionPolicy;
import gobblin.data.management.retention.version.TimestampedDatasetVersion;
import gobblin.data.management.retention.version.finder.ModDateTimeDatasetVersionFinder;
import gobblin.data.management.retention.version.finder.VersionFinder;


/**
 * {@link DatasetBase} for a modification time based dataset.
 *
 * Uses a {@link ModDateTimeDatasetVersionFinder} and a {@link TimeBasedRetentionPolicy}.
 */
public class ModificationTimeDataset extends DatasetBase<TimestampedDatasetVersion> {

  private final VersionFinder<TimestampedDatasetVersion> versionFinder;
  private final RetentionPolicy<TimestampedDatasetVersion> retentionPolicy;
  private final Path datasetRoot;

  public ModificationTimeDataset(FileSystem fs, Properties props, Path datasetRoot) throws IOException {
    this(fs, props, datasetRoot, LoggerFactory.getLogger(ModificationTimeDataset.class));
  }

  public ModificationTimeDataset(FileSystem fs, Properties props, Path datasetRoot, Logger log) throws IOException {
    super(fs, props, log);
    this.versionFinder = new ModDateTimeDatasetVersionFinder(fs, props);
    this.retentionPolicy = new TimeBasedRetentionPolicy(props);
    this.datasetRoot = datasetRoot;
  }

  @Override
  public Path datasetRoot() {
    return this.datasetRoot;
  }

  @Override
  public VersionFinder<? extends TimestampedDatasetVersion> getVersionFinder() {
    return this.versionFinder;
  }

  @Override
  public RetentionPolicy<TimestampedDatasetVersion> getRetentionPolicy() {
    return this.retentionPolicy;
  }

}
