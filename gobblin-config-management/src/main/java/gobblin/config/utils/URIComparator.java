package gobblin.config.utils;

import java.net.URI;
import java.util.Comparator;


public class URIComparator implements Comparator<URI> {

  /**
   * Used to compare input URIs 
   * @param o1 - in {@java.util.TreeMap}, o1 is the key in the map already
   * @param o2 - in {@java.util.TreeMap}, o2 is the key to compare to
   * @return 0 iff both URIs scheme name, authority are the same and o2's path starts with o1's path 
   * If used in TreeMap, 
   */
  @Override
  public int compare(URI o1, URI o2) {
    if (o1 == null || o2 == null)
      return 0;
    int result = 0;

    String scheme1 = o1.getScheme();
    String scheme2 = o2.getScheme();
    result = compareString(scheme1, scheme2);
    if (result != 0)
      return result;

    String auth1 = o1.getAuthority();
    String auth2 = o2.getAuthority();
    result = compareString(auth1, auth2);
    if (result != 0)
      return result;

    String path1 = o1.getPath();
    String path2 = o2.getPath();

    // path2 starts with path1
    if (path1.equals(path2) || path1.indexOf(path2 + "/") == 0) {
      return 0;
    }

    return path1.compareTo(path2);
  }

  private int compareString(String s1, String s2) {
    if (s1 == null && s2 == null)
      return 0;

    if (s1 == null)
      return -1;
    if (s2 == null)
      return 1;

    return s1.compareTo(s2);
  }
}
