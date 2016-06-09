package gobblin.util.filesystem;

import java.io.Serializable;
import java.util.Comparator;

import org.apache.commons.collections.comparators.ReverseComparator;
import org.apache.commons.io.IOCase;
import org.apache.hadoop.fs.Path;


public class PathComparator extends AbstractPathComparator implements Serializable {

  /** Case-sensitive path comparator instance (see {@link IOCase#SENSITIVE}) */
  public static final Comparator<Path> PATH_COMPARATOR = new PathComparator();

  /** Reverse case-sensitive path comparator instance (see {@link IOCase#SENSITIVE}) */
  public static final Comparator<Path> PATH_REVERSE = new ReverseComparator(PATH_COMPARATOR);

  /** Case-insensitive path comparator instance (see {@link IOCase#INSENSITIVE}) */
  public static final Comparator<Path> PATH_INSENSITIVE_COMPARATOR = new PathComparator(IOCase.INSENSITIVE);

  /** Reverse case-insensitive path comparator instance (see {@link IOCase#INSENSITIVE}) */
  public static final Comparator<Path> PATH_INSENSITIVE_REVERSE = new ReverseComparator(PATH_INSENSITIVE_COMPARATOR);

  /** System sensitive path comparator instance (see {@link IOCase#SYSTEM}) */
  public static final Comparator<Path> PATH_SYSTEM_COMPARATOR = new PathComparator(IOCase.SYSTEM);

  /** Reverse system sensitive path comparator instance (see {@link IOCase#SYSTEM}) */
  public static final Comparator<Path> PATH_SYSTEM_REVERSE = new ReverseComparator(PATH_SYSTEM_COMPARATOR);

  /** Whether the comparison is case sensitive. */
  private final IOCase caseSensitivity;

  /**
   * Construct a case sensitive file path comparator instance.
   */
  public PathComparator() {
    this.caseSensitivity = IOCase.SENSITIVE;
  }

  /**
   * Construct a file path comparator instance with the specified case-sensitivity.
   *
   * @param caseSensitivity  how to handle case sensitivity, null means case-sensitive
   */
  public PathComparator(final IOCase caseSensitivity) {
    this.caseSensitivity = caseSensitivity == null ? IOCase.SENSITIVE : caseSensitivity;
  }

  /**
   * Compare the paths of two files the specified case sensitivity.
   *
   * @param path1 The first path(equivalent to file object) to compare
   * @param path2 The second path(equivalent to file object) to compare
   * @return a negative value if the first file's path
   * is less than the second, zero if the paths are the
   * same and a positive value if the first files path
   * is greater than the second file.
   *
   */
  // todo : Not sure whether this toUri is necessary.
  public int compare(final Path path1, final Path path2) {
    return caseSensitivity.checkCompareTo(path1.toUri().toString(), path2.toUri().toString());
  }

  /**
   * String representation of this file comparator.
   *
   * @return String representation of this file comparator
   */
  @Override
  public String toString() {
    return super.toString() + "[caseSensitivity=" + caseSensitivity + "]";
  }
}
