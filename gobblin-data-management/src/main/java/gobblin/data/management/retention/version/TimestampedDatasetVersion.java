package gobblin.data.management.retention.version;

import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

import com.google.common.collect.Sets;


/**
 * {@link gobblin.data.management.retention.version.DatasetVersion} based on a timestamp.
 */
public class TimestampedDatasetVersion implements DatasetVersion {

  private final DateTime version;
  private final Path path;

  public TimestampedDatasetVersion(DateTime version, Path path) {
    this.version = version;
    this.path = path;
  }

  public DateTime getDateTime() {
    return this.version;
  }

  @Override
  public int compareTo(DatasetVersion other) {
    TimestampedDatasetVersion otherAsDateTime = (TimestampedDatasetVersion)other;
    return this.version.compareTo(otherAsDateTime.version);
  }

  @Override
  public String toString() {
    return version.toString(DateTimeFormat.shortDateTime());
  }

  @Override
  public Set<Path> getPathsToDelete() {
    return Sets.newHashSet(this.path);
  }
}
