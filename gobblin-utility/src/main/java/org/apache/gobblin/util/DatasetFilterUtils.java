/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gobblin.util;

import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import gobblin.configuration.State;


/**
 * A utility class for filtering datasets through blacklist and whitelist.
 */
public class DatasetFilterUtils {

  public static List<Pattern> getPatternList(State state, String propKey) {
    return getPatternList(state, propKey, StringUtils.EMPTY);
  }

  public static List<Pattern> getPatternList(State state, String propKey, String def) {
    List<String> list = state.getPropAsList(propKey, def);
    return getPatternsFromStrings(list);
  }

  /**
   * Convert a list of Strings to a list of Patterns.
   */
  public static List<Pattern> getPatternsFromStrings(List<String> strings) {
    List<Pattern> patterns = Lists.newArrayList();
    for (String s : strings) {
      patterns.add(Pattern.compile(s));
    }
    return patterns;
  }

  public static List<String> filter(List<String> topics, List<Pattern> blacklist, List<Pattern> whitelist) {
    List<String> result = Lists.newArrayList();
    for (String topic : topics) {
      if (survived(topic, blacklist, whitelist)) {
        result.add(topic);
      }
    }
    return result;
  }

  public static Set<String> filter(Set<String> topics, List<Pattern> blacklist, List<Pattern> whitelist) {
    Set<String> result = Sets.newHashSet();
    for (String topic : topics) {
      if (survived(topic, blacklist, whitelist)) {
        result.add(topic);
      }
    }
    return result;
  }

  /**
   * A topic survives if (1) it doesn't match the blacklist, and
   * (2) either whitelist is empty, or it matches the whitelist.
   * Whitelist and blacklist use regex patterns (NOT glob patterns).
   */
  public static boolean survived(String topic, List<Pattern> blacklist, List<Pattern> whitelist) {
    if (stringInPatterns(topic, blacklist)) {
      return false;
    }
    return (whitelist.isEmpty() || stringInPatterns(topic, whitelist));
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
