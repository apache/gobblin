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

package org.apache.gobblin.multistage.util;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;


/**
 * a general datetime parsing utility
 *
 * Note: Joda supports only up to milliseconds, if data has microseconds, it will be truncated
 *
 * Note: Joda doesn't like "America/Los_Angeles", but rather it accepts PST or -08:00, therefore
 * long form timezone names are not supported.
 */

public interface DateTimeUtils {
  String DEFAULT_TIMEZONE = "America/Los_Angeles";
  DateTimeFormatter DATE_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd");
  Map<String, DateTimeFormatter> FORMATS = new ImmutableMap.Builder<String, DateTimeFormatter>()
      .put("\\d{4}-\\d{2}-\\d{2}", DateTimeFormat.forPattern("yyyy-MM-dd"))
      .put("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}", DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss"))
      .put("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{1}", DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.S"))
      .put("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{2}", DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SS"))
      .put("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3}", DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS"))
      .put("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{4}", DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSS"))
      .put("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{5}", DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSSS"))
      .put("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{6}", DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSS"))
      .put("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}", DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss"))
      .put("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{1}", DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.S"))
      .put("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{2}", DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SS"))
      .put("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}", DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS"))
      .put("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{4}", DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSS"))
      .put("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{5}", DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSS"))
      .put("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{6}", DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS"))
      .build();
  Map<String, DateTimeFormatter> FORMATS_WITH_ZONE = new ImmutableMap.Builder<String, DateTimeFormatter>()
      // date time string with timezone specified as +/- hh:mm
      .put("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}[-+]+\\d{2}:?\\d{2}", DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ssZ"))
      .put("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{1}[-+]+\\d{2}:?\\d{2}", DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SZ"))
      .put("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{2}[-+]+\\d{2}:?\\d{2}", DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSZ"))
      .put("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3}[-+]+\\d{2}:?\\d{2}", DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSZ"))
      .put("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{4}[-+]+\\d{2}:?\\d{2}", DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSSZ"))
      .put("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{5}[-+]+\\d{2}:?\\d{2}", DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSZ"))
      .put("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{6}[-+]+\\d{2}:?\\d{2}", DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSSZ"))

      // date time string with timezone specified with time zone ids, like PST
      // date time string with timezone specified with long form time zone ids, like America/Los_Angeles, is not working
      .put("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}[a-zA-Z\\/\\_]+", DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ssz"))
      .put("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}.\\d{1}[a-zA-Z\\/\\_]+", DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.Sz"))
      .put("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}.\\d{2}[a-zA-Z\\/\\_]+", DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSz"))
      .put("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}.\\d{3}[a-zA-Z\\/\\_]+", DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSz"))
      .put("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}.\\d{4}[a-zA-Z\\/\\_]+", DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSSz"))
      .put("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}.\\d{5}[a-zA-Z\\/\\_]+", DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSz"))
      .put("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}.\\d{6}[a-zA-Z\\/\\_]+", DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSSz"))

      // date time string with timezone specified as +/- hh:mm
      .put("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}[-+]+\\d{2}:?\\d{2}", DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ssZ"))
      .put("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{1}[-+]+\\d{2}:?\\d{2}", DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SZ"))
      .put("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{2}[-+]+\\d{2}:?\\d{2}", DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSZ"))
      .put("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}[-+]+\\d{2}:?\\d{2}", DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ"))
      .put("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{4}[-+]+\\d{2}:?\\d{2}", DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSZ"))
      .put("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{5}[-+]+\\d{2}:?\\d{2}", DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSZ"))
      .put("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{6}[-+]+\\d{2}:?\\d{2}", DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSZ"))

      // date time string with timezone specified with short form time zone ids, like PST
      // date time string with timezone specified with long form time zone ids, like America/Los_Angeles, is not working
      .put("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}[a-zA-Z\\/\\_]+", DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ssz"))
      .put("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{1}[a-zA-Z\\/\\_]+", DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.Sz"))
      .put("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{2}[a-zA-Z\\/\\_]+", DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSz"))
      .put("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}[a-zA-Z\\/\\_]+", DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSz"))
      .put("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{4}[a-zA-Z\\/\\_]+", DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSz"))
      .put("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{5}[a-zA-Z\\/\\_]+", DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSz"))
      .put("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{6}[a-zA-Z\\/\\_]+", DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSz"))
      .build();

  static DateTime parse(String dtString) {
    return parse(dtString, DEFAULT_TIMEZONE);
  }

  /**
   * Parse the date time string against a predefined list of formats. If none of them match,
   * the input string is truncated to first 10 characters in hope of matching to basic ISO date
   * format of yyyy-MM-dd
   * @param dtString the date time value string
   * @param timezone the timezone of the string
   * @return the parsed Date Time object
   */
  static DateTime parse(String dtString, String timezone) {
    DateTimeZone timeZone = DateTimeZone.forID(timezone.isEmpty() ? DEFAULT_TIMEZONE : timezone);
    try {
      for (String format : FORMATS.keySet()) {
        if (dtString.matches(format)) {
          return FORMATS.get(format).withZone(timeZone).parseDateTime(dtString);
        }
      }
      // ignore timezone parameter if the date time string itself has time zone information
      for (String format : FORMATS_WITH_ZONE.keySet()) {
        if (dtString.matches(format)) {
          return FORMATS_WITH_ZONE.get(format).parseDateTime(dtString);
        }
      }
    } catch (Exception e) {
      return DATE_FORMATTER.withZone(timeZone).parseDateTime(dtString.substring(0, 10));
    }
    return DATE_FORMATTER.withZone(timeZone).parseDateTime(dtString.substring(0, 10));
  }
}
