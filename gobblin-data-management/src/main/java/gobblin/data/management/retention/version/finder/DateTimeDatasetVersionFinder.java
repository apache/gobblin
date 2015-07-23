package gobblin.data.management.retention.version.finder;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import azkaban.utils.Props;

import gobblin.data.management.retention.DatasetCleaner;
import gobblin.data.management.retention.version.DatasetVersion;
import gobblin.data.management.retention.version.TimestampedDatasetVersion;


/**
 * {@link gobblin.data.management.retention.version.finder.DatasetVersionFinder} for timestamped datasets. Uses a datetime pattern
 * to find dataset versions and parse the {@link org.joda.time.DateTime} representing the version.
 */
public class DateTimeDatasetVersionFinder extends DatasetVersionFinder<TimestampedDatasetVersion> {

  private static final Logger LOGGER = LoggerFactory.getLogger(DateTimeDatasetVersionFinder.class);

  public static final String DATE_TIME_PATTERN_KEY = DatasetCleaner.CONFIGURATION_KEY_PREFIX + "datetime.pattern";

  private final Path globPattern;
  private final DateTimeFormatter formatter;

  public DateTimeDatasetVersionFinder(FileSystem fs, Props props) {
    super(fs, props);
    String pattern = props.getString(DATE_TIME_PATTERN_KEY);
    this.globPattern = new Path(pattern.replaceAll("[^/]+", "*"));
    this.formatter = DateTimeFormat.forPattern(pattern);
  }

  @Override
  public Class<? extends DatasetVersion> versionClass() {
    return TimestampedDatasetVersion.class;
  }

  /**
   * Obtained by replacing all non-slash characters in datetime pattern by *.
   * E.g. yyyy/MM/dd/hh/mm -> *\/*\/*\/*\/*
   */
  @Override
  public Path globVersionPattern() {
    return this.globPattern;
  }

  /**
   * Parse {@link org.joda.time.DateTime} from {@link org.apache.hadoop.fs.Path} using datetime pattern.
   */
  @Override
  public TimestampedDatasetVersion getDatasetVersion(Path pathRelativeToDatasetRoot, Path fullPath) {
    try {
      return new TimestampedDatasetVersion(this.formatter.parseDateTime(pathRelativeToDatasetRoot.toString()), fullPath);
    } catch(IllegalArgumentException exception) {
      LOGGER.warn("Candidate dataset version at " + pathRelativeToDatasetRoot
          + " does not match expected pattern. Ignoring.");
      return null;
    }
  }

}
