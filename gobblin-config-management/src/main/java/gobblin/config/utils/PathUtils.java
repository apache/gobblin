package gobblin.config.utils;

import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.fs.Path;

/*
 * Used to resolve the hadoop 1 -> 2 issue
 */
public class PathUtils {
  
  public static Path getParent(URI uri){
    if(uri == null) return null;
    
    String pStr = uri.getPath();
    if(pStr.length()==0) return null;
    
    Path p = (new Path(uri.getPath())).getParent();
    return p;
  }
  
  public static URI getParentURI(URI uri) {
    String pStr = uri.getPath();
    if(pStr.length()==0) return null;

    Path p = (new Path(uri.getPath())).getParent();
    try {
      return new URI(uri.getScheme(), uri.getAuthority(), p.toString(), uri.getQuery(), uri.getFragment());
    } catch (URISyntaxException e) {
      // Should not come here
      e.printStackTrace();
    }
    return null;
  }
}
