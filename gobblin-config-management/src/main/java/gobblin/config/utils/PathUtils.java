package gobblin.config.utils;

import java.net.URI;

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
  
  public static Path getParent(URI uri){
    if(uri == null) return null;
    
    String pStr = uri.getPath();
    if(pStr.length()==0) return null;
    
    Path p = (new Path(uri.getPath())).getParent();
    return p;
  }
}
