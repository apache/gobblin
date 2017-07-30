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

package gobblin.metrics;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;


/**
 * A class representing a dimension or property associated with a {@link Taggable}.
 *
 * @param <T> type of the tag value
 *
 * @author Yinan Li
 */
public class Tag<T> extends AbstractMap.SimpleEntry<String, T> {

  private static final long serialVersionUID = -5083709915031933607L;
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
      return new Tag<>(splitKeyValue.get(0), splitKeyValue.get(1));
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

  /**
   * Converts a {@link Map} of key, value pairs to a {@link List} of {@link Tag}s. Each key, value pair will be used to
   * create a new {@link Tag}.
   *
   * @param tagsMap a {@link Map} of key, value pairs that should be converted into {@link Tag}s
   *
   * @return a {@link List} of {@link Tag}s
   */
  public static <T> List<Tag<T>> fromMap(Map<? extends String, T> tagsMap) {
    ImmutableList.Builder<Tag<T>> tagsBuilder = ImmutableList.builder();
    for (Map.Entry<? extends String, T> entry : tagsMap.entrySet()) {
      tagsBuilder.add(new Tag<>(entry));
    }
    return tagsBuilder.build();
  }

  /**
   * Converts a {@link List} of {@link Tag}s to a {@link Map} of key, value pairs.
   *
   * @param tags a {@link List} of {@link Tag}s that should be converted to key, value pairs
   *
   * @return a {@link Map} of key, value pairs
   */
  public static <T> Map<? extends String, T> toMap(List<Tag<T>> tags) {
    ImmutableMap.Builder<String, T> tagsMapBuilder = ImmutableMap.builder();
    for (Tag<T> tag : tags) {
      tagsMapBuilder.put(tag.getKey(), tag.getValue());
    }
    return tagsMapBuilder.build();
  }

  /**
   * Converts a {@link List} of wildcard {@link Tag}s to a {@link List} of {@link String} {@link Tag}s.
   *
   * @param tags a {@link List} of {@link Tag}s that should be converted to {@link Tag}s with value of type {@link String}
   *
   * @return a {@link List} of {@link Tag}s
   *
   * @see #tagValueToString(Tag)
   */
  public static List<Tag<String>> tagValuesToString(List<? extends Tag<?>> tags) {
    return Lists.transform(tags, new Function<Tag<?>, Tag<String>>() {
      @Nullable @Override public Tag<String> apply(Tag<?> input) {
        return input == null ? null : Tag.tagValueToString(input);
      }
    });
  }

  /**
   * Converts a wildcard {@link Tag} to a {@link String} {@link Tag}. This method uses the {@link Object#toString()}
   * method to convert the wildcard type to a {@link String}.
   *
   * @param tag a {@link Tag} that should be converted to a {@link Tag} with value of type {@link String}
   *
   * @return a {@link Tag} with a {@link String} value
   */
  public static Tag<String> tagValueToString(Tag<?> tag) {
    return new Tag<>(tag.getKey(), tag.getValue().toString());
  }

  @Override
  public String toString() {
    return getKey() + KEY_VALUE_SEPARATOR + getValue();
  }
}
