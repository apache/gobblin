package gobblin.data.management.retention.version.finder;

import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.typesafe.config.Config;

import gobblin.data.management.retention.version.DatasetVersion;
import gobblin.data.management.retention.version.StringDatasetVersion;


/**
 * @deprecated
 * See javadoc for {@link gobblin.data.management.version.finder.WatermarkDatasetVersionFinder}.
 */
@Deprecated
public class WatermarkDatasetVersionFinder extends DatasetVersionFinder<StringDatasetVersion> {

  private final gobblin.data.management.version.finder.WatermarkDatasetVersionFinder realVersionFinder;

  public static final String WATERMARK_REGEX_KEY =
      gobblin.data.management.version.finder.WatermarkDatasetVersionFinder.RETENTION_WATERMARK_REGEX_KEY;

  public WatermarkDatasetVersionFinder(FileSystem fs, Properties props) {
    super(fs, props);
    this.realVersionFinder = new gobblin.data.management.version.finder.WatermarkDatasetVersionFinder(fs, props);
  }

  public WatermarkDatasetVersionFinder(FileSystem fs, Config config) {
    super(fs);
    this.realVersionFinder = new gobblin.data.management.version.finder.WatermarkDatasetVersionFinder(fs, config);
  }

  @Override
  public Class<? extends DatasetVersion> versionClass() {
    return StringDatasetVersion.class;
  }

  @Override
  public Path globVersionPattern() {
    return this.realVersionFinder.globVersionPattern();
  }

  @Override
  public StringDatasetVersion getDatasetVersion(Path pathRelativeToDatasetRoot, Path fullPath) {
    return new StringDatasetVersion(this.realVersionFinder.getDatasetVersion(pathRelativeToDatasetRoot, fullPath));
  }
}
