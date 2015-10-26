package gobblin.data.management.util.filters;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;


public class GpgPathFilter implements PathFilter {

  @Override
  public boolean accept(Path path) {
    return path.getName().endsWith(".tar.gz.gpg") && path.getName().startsWith("Log");
  }
}
