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

package org.apache.gobblin.util;

import com.codahale.metrics.Timer;
import java.io.Closeable;
import java.io.IOException;


/**
 * This class is a wrapper on com.codahale.metrics.Timer.Context
 * and implements Closable. Timer.Context implements AutoCloseable 
 * (with io.dropwizard.metrics library update from 3.x.y to 4.x.y)
 * and this wrapper class allows caller to still continue using Closable.
 */
public class ClosableTimerContext implements Closeable {
  private final Timer.Context timerContext;

  public ClosableTimerContext(final Timer.Context timerContext) {
    this.timerContext = timerContext;
  }
  
  @Override
  public void close() throws IOException {
    timerContext.close();
  }
}
