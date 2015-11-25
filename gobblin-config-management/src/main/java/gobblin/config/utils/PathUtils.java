package gobblin.config.utils;

import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.fs.Path;


/*
 * Used to resolve the hadoop 1 -> 2 issue
 */
public class PathUtils {

  public static Path getParent(URI uri) {
    if (uri == null)
      return null;

    String pStr = uri.getPath();
    if (pStr.length() == 0)
      return null;

    Path p = (new Path(uri.getPath())).getParent();
    return p;
  }

  public static URI getParentURI(URI uri) {
    String pStr = uri.getPath();
    if (pStr.length() == 0)
      return null;

    Path p = (new Path(uri.getPath())).getParent();
    try {
      return new URI(uri.getScheme(), uri.getAuthority(), p.toString(), uri.getQuery(), uri.getFragment());
    } catch (URISyntaxException e) {
      // Should not come here
      e.printStackTrace();
    }
    return null;
  }

  /**
   * @param parent - parent URI
   * @param child - descendant URI
   * @return true if o2 is the descendant of o1 
   */
  public static boolean checkDescendant(URI parent, URI child) {
    if (parent == null && child == null) {
      return true;
    }

    if (parent == null || child == null) {
      return false;
    }

    boolean result = false;
    String scheme1 = parent.getScheme();
    String scheme2 = child.getScheme();
    result = stringEquals(scheme1, scheme2);
    if (!result)
      return result;

    String auth1 = parent.getAuthority();
    String auth2 = child.getAuthority();
    result = stringEquals(auth1, auth2);
    if (!result)
      return result;

    String path1 = parent.getPath();
    String path2 = child.getPath();

    // path2 starts with path1
    if (path1.equals(path2) || path1.indexOf(path2 + "/") == 0) {
      return true;
    }
    return false;
  }

  public static boolean stringEquals(String s1, String s2) {
    if (s1 == null && s2 == null)
      return true;

    if (s1 == null || s2 == null)
      return false;

    return s1.equals(s2);
  }
}
