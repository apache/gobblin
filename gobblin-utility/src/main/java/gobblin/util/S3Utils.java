/* (c) 2015 NerdWallet All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */
package gobblin.util;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A util class for dealing with S3
 *
 * @author ahollenbach@nerdwallet.com
 */
public class S3Utils {
  private static final Logger LOG = LoggerFactory.getLogger(S3Utils.class);

  /**
   * If you want a your S3 path (or any string) to contain a date, you can replace it here
   * The placeholder is what it looks for in the path, and replaces it
   * with the date (offset by S3_*_DATE_OFFSET), using the pattern to format it.
   * <p/>
   * If no dates are matched in the path, nothing happens and it returns back
   * the string unchanged.
   * <p/>
   * This also replaces timestamps if you have included that placeholder.
   *
   * @param state  The state
   * @param str A string possibly containing a date placeholder
   * @return the string with any date placeholders replaced with the specified date
   * pattern and offset.
   */
  public static String checkAndReplaceDates(State state, String str) {
    // Replace any now placeholders
    String publisherPlaceholder =
        state.getProp(ConfigurationKeys.S3_PUBLISHER_DATE_PLACEHOLDER, ConfigurationKeys.DEFAULT_S3_PUBLISHER_DATE_PLACEHOLDER);
    String publisherPattern =
        state.getProp(ConfigurationKeys.S3_PUBLISHER_DATE_PATTERN, ConfigurationKeys.DEFAULT_S3_PUBLISHER_DATE_PATTERN);
    str = replaceDate(str, publisherPlaceholder, publisherPattern, 0);


    // Replace any of the date placeholders
    String datePlaceholder =
        state.getProp(ConfigurationKeys.S3_SOURCE_DATE_PLACEHOLDER, ConfigurationKeys.DEFAULT_S3_SOURCE_DATE_PLACEHOLDER);
    String datePattern = state.getProp(ConfigurationKeys.S3_SOURCE_DATE_PATTERN, ConfigurationKeys.DEFAULT_S3_SOURCE_DATE_PATTERN);
    // If set, 0 for today, -1 for yesterday, etc.
    int dateOffset = state.getPropAsInt(ConfigurationKeys.S3_SOURCE_DATE_OFFSET, ConfigurationKeys.DEFAULT_S3_SOURCE_DATE_OFFSET);
    str = replaceDate(str, datePlaceholder, datePattern, dateOffset);


    // Replace any timestamp placeholders
    String timestampPlaceholder = ConfigurationKeys.S3_TIMESTAMP_PLACEHOLDER;
    String timestampPattern =
        state.getProp(ConfigurationKeys.S3_TIMESTAMP_PATTERN, ConfigurationKeys.DEFAULT_S3_TIMESTAMP_PATTERN);
    str = replaceDate(str, timestampPlaceholder, timestampPattern, 0);

    return str;
  }

  /**
   * Replaces a placeholder with the date using the pattern and offset supplied, relative to the current time.
   *
   * @param input An input string containing 0 or more of the datePlaceholder
   * @param datePlaceholder A placeholder string to match on
   * @param datePattern The pattern to use when formatting the date
   * @param dateOffset The offset to the current time (0 for current, -1 for one day ago, etc.)
   * @return The input with all of the given placeholder replaced with the date formatted as specified.
   */
  private static String replaceDate(String input, String datePlaceholder, String datePattern, int dateOffset) {
    SimpleDateFormat df = new SimpleDateFormat(datePattern);
    Calendar cal = Calendar.getInstance();
    cal.add(Calendar.DATE, dateOffset);

    return input.replace(datePlaceholder, df.format(cal.getTime()));
  }
}
