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

package org.apache.gobblin.test;

import java.util.Random;
import java.util.regex.Pattern;

import com.typesafe.config.Config;

import lombok.Builder;

import org.apache.gobblin.util.ConfigUtils;


/**
 * A class that can be configured to simulate errors
 */
public class ErrorManager<D> {


  public enum ErrorType
  {
    RANDOM,
    NTH,
    REGEX,
    ALL
  };


  private static final int DEFAULT_N = 5;
  private final ErrorType errorType;
  private final Random random = new Random();
  private final Pattern pattern;
  private final int num;
  private int index = 0;

  public static final String ERROR_TYPE_CONFIGURATION_KEY="flaky.errorType";
  static final String FLAKY_ERROR_EVERY_CONFIGURATION_KEY = "flaky.errorEvery";
  static final String FLAKY_ERROR_REGEX_PATTERN_KEY = "flaky.regexPattern" ;
  private static final ErrorType DEFAULT_ERROR_TYPE = ErrorType.RANDOM;

  private static ErrorType getType(Config config) {
    String type = ConfigUtils.getString(config, ERROR_TYPE_CONFIGURATION_KEY, "");
    ErrorType errorType;
    if (!type.isEmpty())
    {
      errorType = ErrorType.valueOf(type.toUpperCase());
    }
    else {
      errorType = DEFAULT_ERROR_TYPE;
    }
    return errorType;
  }

  private static int getNum(Config config)
  {
    return ConfigUtils.getInt(config, FLAKY_ERROR_EVERY_CONFIGURATION_KEY, DEFAULT_N);
  }

  private static String getPattern(Config config)
  {
    return ConfigUtils.getString(config, FLAKY_ERROR_REGEX_PATTERN_KEY, null);
  }

  public ErrorManager(Config config) {
    this(getType(config), getNum(config), getPattern(config));
  }

  @Builder
  public ErrorManager(ErrorType errorType, int errorEvery, String pattern)
  {
    this.errorType = errorType;
    this.num = errorEvery;
    this.pattern = (pattern == null)?null:Pattern.compile(pattern);
  }

  public boolean nextError(D record) {
    switch (errorType) {
      case ALL:
        return true;
      case NTH:
        return (++index % num == 0);
      case RANDOM:
        return random.nextBoolean();
      case REGEX:
        return pattern.matcher(record.toString()).find();
      default:
        throw new IllegalStateException("Unexpected error type: " + errorType.toString());
    }
  }
}
