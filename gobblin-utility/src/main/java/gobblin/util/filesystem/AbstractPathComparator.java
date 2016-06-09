package gobblin.util.filesystem;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.fs.Path;


abstract class AbstractPathComparator implements Comparator<Path> {
  public Path[] sort(final Path... paths) {
    if (paths != null) {
      Arrays.sort(paths, this);
    }
    return paths;
  }

  public List<Path> sort(final List<Path> paths) {
    if (paths != null) {
      Collections.sort(paths, this);
    }
    return paths;
  }

  /**
   * String representation of this file comparator.
   *
   * @return String representation of this file comparator
   */
  @Override
  public String toString() {
    return getClass().getSimpleName();
  }
}
