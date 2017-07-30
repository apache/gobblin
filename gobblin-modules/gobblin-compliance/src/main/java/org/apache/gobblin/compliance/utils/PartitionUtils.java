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
package gobblin.compliance.utils;

import java.util.Map;

import com.google.common.base.Preconditions;

import gobblin.compliance.ComplianceConfigurationKeys;


/**
 * A utility class for Partition.
 *
 * @author adsharma
 */
public class PartitionUtils {
  private static final String SINGLE_QUOTE = "'";

  /**
   * Add single quotes to the string, if not present.
   * TestString will be converted to 'TestString'
   */
  public static String getQuotedString(String st) {
    Preconditions.checkNotNull(st);
    String quotedString = "";
    if (!st.startsWith(SINGLE_QUOTE)) {
      quotedString += SINGLE_QUOTE;
    }
    quotedString += st;
    if (!st.endsWith(SINGLE_QUOTE)) {
      quotedString += SINGLE_QUOTE;
    }
    return quotedString;
  }

  /**
   * This method returns the partition spec string of the partition.
   * Example : datepartition='2016-01-01-00', size='12345'
   */
  public static String getPartitionSpecString(Map<String, String> spec) {
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, String> entry : spec.entrySet()) {
      if (!sb.toString().isEmpty()) {
        sb.append(",");
      }
      sb.append(entry.getKey());
      sb.append("=");
      sb.append(getQuotedString(entry.getValue()));
    }
    return sb.toString();
  }

  /**
   * Check if a given string is a valid unixTimeStamp
   */
  public static boolean isUnixTimeStamp(String timeStamp) {
    if (timeStamp.length() != ComplianceConfigurationKeys.TIME_STAMP_LENGTH) {
      return false;
    }
    try {
      Long.parseLong(timeStamp);
      return true;
    } catch (NumberFormatException e) {
      return false;
    }
  }
}
