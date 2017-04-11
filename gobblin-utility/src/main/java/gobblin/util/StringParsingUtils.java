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

import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class StringParsingUtils {

  public static final Pattern HUMAN_READABLE_SIZE_PATTERN = Pattern.compile("([0-9\\.]+)\\s*([kKmMgGtTpP]?)[bB]?");

  /**
   * Convert a human readable string (e.g. 10kb) into the number of bytes.
   *
   * Examples: 10b, 10kb, 10mb, 10gb, 10tb, 10pb, 1.2m, ...
   */
  public static long humanReadableToByteCount(String string) throws FormatException {
    Matcher matcher = HUMAN_READABLE_SIZE_PATTERN.matcher(string.trim());
    if (!matcher.matches()) {
      throw new FormatException("Could not parse human readable size string " + string);
    }
    int exponent = 0;
    switch (matcher.group(2).toUpperCase()) {
      case "":
        exponent = 0;
        break;
      case "K":
        exponent = 10;
        break;
      case "M":
        exponent = 20;
        break;
      case "G":
        exponent = 30;
        break;
      case "T":
        exponent = 40;
        break;
      case "P":
        exponent = 50;
        break;
      default:
        throw new FormatException("Could not parse human readable size string " + string);
    }
    try {
      double base = Double.parseDouble(matcher.group(1));
      return (long) (base * (1L << exponent));
    } catch (NumberFormatException nfe) {
      throw new FormatException("Could not parse human readable size string " + string);
    }
  }

  public static class FormatException extends Exception {
    public FormatException(String message) {
      super(message);
    }

    public FormatException(String message, Throwable cause) {
      super(message, cause);
    }

    public FormatException(Throwable cause) {
      super(cause);
    }
  }

}
