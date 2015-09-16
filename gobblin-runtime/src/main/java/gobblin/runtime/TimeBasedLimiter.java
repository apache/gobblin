/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.runtime;

import java.util.concurrent.TimeUnit;


/**
 * {@inheritDoc}
 *
 * @deprecated This class has been moved to {@link gobblin.util.limiter.TimeBasedLimiter}.
 */
@Deprecated
public class TimeBasedLimiter extends gobblin.util.limiter.TimeBasedLimiter {
  public TimeBasedLimiter(long timeLimit) {
    super(timeLimit);
  }

  public TimeBasedLimiter(long timeLimit, TimeUnit timeUnit) {
    super(timeLimit, timeUnit);
  }
}
