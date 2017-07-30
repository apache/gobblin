/*
 * Copyright (C) 2014-2017 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.util.executors;

import org.slf4j.MDC;

import java.util.Map;
import java.util.concurrent.Callable;

public class MDCPropagatingCallable<T> implements Callable<T> {
  private final Callable<T> callable;
  private final Map<String, String> context;

  public MDCPropagatingCallable(Callable<T> callable) {
    this.callable = callable;
    this.context = MDC.getCopyOfContextMap();
  }

  @Override
  public T call() throws Exception {
    T answer;
    Map<String, String> originalContext = MDC.getCopyOfContextMap();
    if (context != null) {
      MDC.setContextMap(context);
    }

    try {
      answer = this.callable.call();
    } finally {
      if (originalContext != null) {
        MDC.setContextMap(originalContext);
      } else {
        MDC.clear();
      }
    }

    return answer;
  }
}
