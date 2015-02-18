/* (c) 2014 LinkedIn Corp. All rights reserved.
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;


/**
 * A type of {@link java.lang.Thread.UncaughtExceptionHandler} that logs uncaught exceptions.
 *
 * @author ynli
 */
public class LoggingUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {

  private final Logger logger;

  public LoggingUncaughtExceptionHandler(Optional<Logger> logger) {
    if (logger.isPresent()) {
      this.logger = logger.get();
    } else {
      this.logger = LoggerFactory.getLogger(LoggingUncaughtExceptionHandler.class);
    }
  }

  @Override
  public void uncaughtException(Thread t, Throwable e) {
    this.logger.error(String.format("Thread %s threw an uncaught exception: %s", t, e), e);
  }
}
