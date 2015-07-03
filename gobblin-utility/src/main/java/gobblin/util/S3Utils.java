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
import gobblin.configuration.SourceState;
import gobblin.configuration.State;
import java.text.SimpleDateFormat;
import java.util.Calendar;


/**
 * A util class for dealing with S3
 *
 * @author ahollenbach@nerdwallet.com
 */
public class S3Utils {

  /**
   * If you want a your S3 path (or any string) to contain a date, you can replace it here
   * The placeholder is what the source looks for in the path, and replaces it
   * with the date (offset by S3_DATE_OFFSET), using the pattern to format it.
   * <p/>
   * If no date is matched in the path, nothing happens and it returns back
   * the string unchanged.
   *
   * @param state  The state
   * @param str A string possibly containing a date placeholder
   * @return the string with any date placeholders replaced with the specified date
   * pattern and offset.
   */
  public static String checkAndReplaceDate(State state, String str) {
    String placeholder =
        state.getProp(ConfigurationKeys.S3_DATE_PLACEHOLDER, ConfigurationKeys.DEFAULT_S3_DATE_PLACEHOLDER);
    String datePattern = state.getProp(ConfigurationKeys.S3_DATE_PATTERN, ConfigurationKeys.DEFAULT_S3_DATE_PATTERN);
    // If set, 0 for today, -1 for yesterday, etc.
    int dateOffset = state.getPropAsInt(ConfigurationKeys.S3_DATE_OFFSET, ConfigurationKeys.DEFAULT_S3_DATE_OFFSET);

    SimpleDateFormat df = new SimpleDateFormat(datePattern);
    Calendar cal = Calendar.getInstance();
    cal.add(Calendar.DATE, dateOffset);

    return str.replace(placeholder, df.format(cal.getTime()));
  }
}
