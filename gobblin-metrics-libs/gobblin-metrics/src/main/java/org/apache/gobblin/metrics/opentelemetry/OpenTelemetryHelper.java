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

package org.apache.gobblin.metrics.opentelemetry;

import java.util.Map;

import lombok.experimental.UtilityClass;
import org.apache.commons.lang3.StringUtils;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;

/**
 * Utility class for OpenTelemetry related operations.
 *
 * <p>Provides methods to handle OpenTelemetry attributes, including merging multiple
 * {@link Attributes} instances and converting maps to {@link Attributes}.
 */
@UtilityClass
public class OpenTelemetryHelper {

  private static final String DEFAULT_OPENTELEMETRY_ATTRIBUTE_VALUE = "UNKNOWN";

  /**
   * Returns the provided attribute value when it is non-null and non-empty;
   * otherwise returns the default OpenTelemetry attribute placeholder.
   *
   * @param value candidate attribute value to check
   * @return the original value if not empty, or DEFAULT_OPENTELEMETRY_ATTRIBUTE_VALUE otherwise
   */
  public static String getOrDefaultOpenTelemetryAttrValue(String value) {
    return StringUtils.defaultIfBlank(value, DEFAULT_OPENTELEMETRY_ATTRIBUTE_VALUE);
  }

  /**
   * Merges multiple {@link Attributes} instances into a single {@link Attributes}.
   *
   * <p>Any {@code null} or empty ({@link Attributes#isEmpty()}) instances are ignored.
   * The resulting {@link Attributes} contains all key-value pairs from the
   * provided non-null, non-empty inputs in the order they are given.
   * For duplicate keys, the last occurrence in the array will take precedence.
   *
   * @param attributesArray array of {@link Attributes} to merge; may contain {@code null} or empty entries
   * @return a new {@link Attributes} instance containing all entries from the non-null,
   *         non-empty inputs; never {@code null}
   */
  public static Attributes mergeAttributes(Attributes... attributesArray) {
    AttributesBuilder builder = Attributes.builder();
    for (Attributes attrs : attributesArray) {
      if (attrs != null && !attrs.isEmpty()) {
        builder.putAll(attrs);
      }
    }
    return builder.build();
  }

  /**
   * Converts a map of string attributes to an OpenTelemetry {@link Attributes} instance.
   *
   * <p>Each entry in the map is converted to an OpenTelemetry attribute, using
   * {@link #getOrDefaultOpenTelemetryAttrValue(String)} to handle empty values.
   *
   * @param attributes map of string attributes to convert; may be {@code null}
   * @return a new {@link Attributes} instance containing the converted attributes;
   *         never {@code null}
   */
  public static Attributes toOpenTelemetryAttributes(Map<String, String> attributes) {
    AttributesBuilder builder = Attributes.builder();
    if (attributes != null) {
      for (Map.Entry<String, String> entry : attributes.entrySet()) {
        String key = entry.getKey();
        String value = getOrDefaultOpenTelemetryAttrValue(entry.getValue());
        builder.put(key, value);
      }
    }
    return builder.build();
  }

}
