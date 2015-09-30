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

package gobblin.util.executors;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;


/**
 * {@link java.util.concurrent.RejectedExecutionHandler} that just re-attempts to put into the queue.
 */
public class ForceQueuePolicy implements RejectedExecutionHandler {
  public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
    try {
      executor.getQueue().put(r);
    } catch (InterruptedException e) {
      //should never happen since we never wait
      throw new RejectedExecutionException(e);
    }
  }
}
