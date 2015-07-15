package gobblin.data.management.retention.version;

import java.util.Set;

import org.apache.hadoop.fs.Path;

import com.google.common.collect.Sets;


/**
 * Dataset version using {@link java.lang.String} as version.
 */
public class StringDatasetVersion implements DatasetVersion {

  private final String version;
  private final Path path;

  public StringDatasetVersion(String version, Path path) {
    this.version = version;
    this.path = path;
  }

  @Override
  public int compareTo(DatasetVersion other) {
    StringDatasetVersion otherAsString = (StringDatasetVersion)other;
    return this.version.equals(otherAsString.version) ?
        this.path.compareTo(otherAsString.path) :
        this.version.compareTo(otherAsString.version);
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof StringDatasetVersion && compareTo((StringDatasetVersion) obj) == 0;
  }

  @Override
  public int hashCode() {
    return this.path.hashCode() + this.version.hashCode();
  }

  @Override
  public String toString() {
    return this.version;
  }

  public String getVersion() {
    return this.version;
  }

  @Override
  public Set<Path> getPathsToDelete() {
    return Sets.newHashSet(this.path);
  }
}
