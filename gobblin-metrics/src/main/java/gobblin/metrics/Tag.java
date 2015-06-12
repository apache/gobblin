/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.metrics;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Splitter;


/**
 * A class representing a dimension or property associated with a {@link Taggable}.
 *
 * @param <T> type of the tag value
 *
 * @author ynli
 */
public class Tag<T> extends AbstractMap.SimpleEntry<String, T> {

  private static final char KEY_VALUE_SEPARATOR = ':';

  /**
   * Reverse of Tag.toString(). Parses a string of the form "key:value" into a {@link Tag}.
   *
   * <p>
   *   If there are multiple ":" in the input string, the key will be the substring up to the first ":", and the
   *   value will be the substring after the first ":".
   * </p>
   *
   * @param tagKeyValue String of the form "key:value".
   * @return {@link gobblin.metrics.Tag} parsed from input.
   */
  public static Tag<String> fromString(String tagKeyValue) {
    List<String> splitKeyValue = Splitter.on(KEY_VALUE_SEPARATOR).limit(2).omitEmptyStrings().splitToList(tagKeyValue);
    if(splitKeyValue.size() == 2) {
      return new Tag<String>(splitKeyValue.get(0), splitKeyValue.get(1));
    } else {
      return null;
    }
  }

  public Tag(String key, T value) {
    super(key, value);
  }

  public Tag(Map.Entry<? extends String, ? extends T> entry) {
    super(entry);
  }

  @Override
  public String toString() {
    return getKey() + KEY_VALUE_SEPARATOR + getValue();
  }
}
