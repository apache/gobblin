package gobblin.config.utils;

import org.apache.hadoop.fs.Path;

/*
 * Used to resolve the hadoop 1 -> 2 issue
 */
public class PathUtils {
  
  public static Path getPathWithoutSchemeAndAuthority(Path path) {
    // This code depends on Path.toString() to remove the leading slash before
    // the drive specification on Windows.
    Path newPath = path.isAbsolute() ? new Path(null, null, path.toUri().getPath()) : path;
    return newPath;
  }
}
