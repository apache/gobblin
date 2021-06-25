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
package org.apache.gobblin.data.management.copy;

import java.util.Properties;

import lombok.extern.slf4j.Slf4j;

import org.joda.time.DateTime;
import org.joda.time.Period;


/**
 * A {@link CopyableFileFilter} that drops a {@link CopyableFile} if file modification time not within the lookback
 * window
 *  <code>sourceFs<code>
 */
@Slf4j
public class ModifiedDateRangeBasedFileFilter extends DateRangeBasedFileFilter {

  public static final String MODIFIED_MIN_LOOK_BACK_TIME_KEY =
      CONFIGURATION_KEY_PREFIX + "selection.modified.min.lookbackTime";
  public static final String MODIFIED_MAX_LOOK_BACK_TIME_KEY =
      CONFIGURATION_KEY_PREFIX + "selection.modified.max.lookbackTime";
  public static final String DATE_PATTERN_TIMEZONE_KEY = CONFIGURATION_KEY_PREFIX + "datetime.timezone";

  public ModifiedDateRangeBasedFileFilter(Properties properties) {
    super(getMinLookbackTime(properties), getMaxLookbackTime(properties),
        properties.getProperty(DATE_PATTERN_TIMEZONE_KEY));
  }

  private static Period getMinLookbackTime(Properties properties) {
    return properties.containsKey(MODIFIED_MIN_LOOK_BACK_TIME_KEY) ? getPeriodFormatter().parsePeriod(
        properties.getProperty(MODIFIED_MIN_LOOK_BACK_TIME_KEY)) : new Period(DateTime.now().getMillis());
  }

  private static Period getMaxLookbackTime(Properties properties) {
    return properties.containsKey(MODIFIED_MAX_LOOK_BACK_TIME_KEY) ? getPeriodFormatter().parsePeriod(
        properties.getProperty(MODIFIED_MAX_LOOK_BACK_TIME_KEY)) : new Period(DateTime.now().minusDays(1).getMillis());
  }
}
