/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.kafka;

import java.util.Random;
import java.util.regex.Pattern;

import com.typesafe.config.Config;

import gobblin.util.ConfigUtils;


/**
 * A class that can be configured to simulate errors
 * {@see FlakyKafkaProducer} for usage.
 */
public class ErrorManager<D> {


  enum ErrorType
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

  ErrorManager(Config config) {
    String type = ConfigUtils.getString(config, ERROR_TYPE_CONFIGURATION_KEY, "");
    num = ConfigUtils.getInt(config, FLAKY_ERROR_EVERY_CONFIGURATION_KEY, DEFAULT_N);
    String patternStr = ConfigUtils.getString(config, FLAKY_ERROR_REGEX_PATTERN_KEY, null);
    if (null == patternStr)
    {
      pattern = null;
    }
    else
    {
      pattern = Pattern.compile(patternStr);
    }
    if (!type.isEmpty())
    {
      errorType = ErrorType.valueOf(type.toUpperCase());
    }
    else {
      errorType = DEFAULT_ERROR_TYPE;
    }
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
