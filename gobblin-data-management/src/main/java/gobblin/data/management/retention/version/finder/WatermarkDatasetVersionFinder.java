package gobblin.data.management.retention.version.finder;

import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Optional;

import javax.annotation.Nullable;

import gobblin.data.management.retention.DatasetCleaner;
import gobblin.data.management.retention.version.DatasetVersion;
import gobblin.data.management.retention.version.StringDatasetVersion;


/**
 * Finds watermarked dataset versions as direct subdirectories of the dataset directory. The watermark is assumed
 * to be part of the subdirectory name. By default, the watermark is the subdirectory name itself, but a regular
 * expression can be provided to extract the watermark from the name. The watermarks will be sorted by String
 * sorting.
 *
 * <p>
 *   For example, snapshots of a database can be named by the unix timestamp when the snapshot was dumped:
 *   /path/to/snapshots/1436223009-snapshot
 *   /path/to/snapshots/1436234210-snapshot
 *   In this case the versions are 1436223009-snapshot, 1436234210-snapshot. Since the watermark is at the
 *   beginning of the name, the natural string ordering is good enough to sort the snapshots, so no regexp is
 *   required to extract the actual watermark.
 * </p>
 */
public class WatermarkDatasetVersionFinder extends DatasetVersionFinder<StringDatasetVersion> {

  public static final Logger LOGGER = LoggerFactory.getLogger(WatermarkDatasetVersionFinder.class);

  public static final String WATERMARK_REGEX_KEY = DatasetCleaner.CONFIGURATION_KEY_PREFIX + "watermark.regex";

  private Optional<Pattern> pattern;

  public WatermarkDatasetVersionFinder(FileSystem fs, Properties props) {
    super(fs, props);
    if(props.containsKey(WATERMARK_REGEX_KEY)) {
      this.pattern = Optional.of(props.getProperty(WATERMARK_REGEX_KEY)).transform(new Function<String, Pattern>() {
        @Nullable
        @Override
        public Pattern apply(String input) {
          return Pattern.compile(input);
        }
      });
    } else {
      this.pattern = Optional.absent();
    }
  }

  @Override
  public Class<? extends DatasetVersion> versionClass() {
    return StringDatasetVersion.class;
  }

  @Override
  public Path globVersionPattern() {
    return new Path("*");
  }

  @Override
  public StringDatasetVersion getDatasetVersion(Path pathRelativeToDatasetRoot, Path fullPath) {
    if(this.pattern.isPresent()) {
      Matcher matcher = this.pattern.get().matcher(pathRelativeToDatasetRoot.getName());
      if(!matcher.find() || matcher.groupCount() < 1) {
        LOGGER.warn("Candidate dataset version at " + pathRelativeToDatasetRoot
            + " does not match expected pattern. Ignoring.");
        return null;
      }
      return new StringDatasetVersion(matcher.group(1), fullPath);
    } else {
      return new StringDatasetVersion(pathRelativeToDatasetRoot.getName(), fullPath);
    }
  }

}
