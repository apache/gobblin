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

package org.apache.gobblin.writer.test;

import java.lang.instrument.Instrumentation;


/**
 * An instrumentation agent for measuring java object memory usage.
 * To use the static method {@link #getObjectSize(Object)}, one needs to turn on jvm arguments as below:
 * -javaagent:"/<PATH_TO_YOUR_MULTIPRODUCT>/gobblin-proxy_trunk/gobblin-github/build/gobblin-core/libs/gobblin-core-<VERSION>.jar
 */
public class InstrumentationAgent {
  private static volatile Instrumentation globalInstrumentation;

  public static void premain(final String agentArgs, final Instrumentation inst) {
    globalInstrumentation = inst;
  }

  public static long getObjectSize(final Object object) {
    if (globalInstrumentation == null) {
      throw new IllegalStateException("Agent not initialized.");
    }
    return globalInstrumentation.getObjectSize(object);
  }
}
