package gobblin.util;

import java.util.List;
import java.util.regex.Pattern;

import com.google.common.collect.Lists;


/**
 * A utility class for filtering datasets through blacklist and whitelist.
 */
public class DatasetFilterUtils {

  /**
   * Convert a list of Strings to a list of Patterns.
   */
  public static List<Pattern> getPatternsFromStrings(List<String> strings) {
    List<Pattern> patterns = Lists.newArrayList();
    for (String s : strings) {
      patterns.add(Pattern.compile(s, Pattern.CASE_INSENSITIVE));
    }
    return patterns;
  }

  /**
   * If whitelist is non-empty, a topic survives if it matches the whitelist.
   * Otherwise, a topic survives if it doesn't match the blacklist.
   * Whitelist and blacklist use regex patterns (NOT glob patterns).
   */
  public static boolean survived(String topic, List<Pattern> blacklist, List<Pattern> whitelist) {
    if (!whitelist.isEmpty()) {
      return stringInPatterns(topic, whitelist);
    } else {
      return !stringInPatterns(topic, blacklist);
    }
  }

  /**
   * Determines whether a string matches one of the regex patterns.
   */
  public static boolean stringInPatterns(String s, List<Pattern> patterns) {
    for (Pattern pattern : patterns) {
      if (pattern.matcher(s).matches()) {
        return true;
      }
    }
    return false;
  }
}
