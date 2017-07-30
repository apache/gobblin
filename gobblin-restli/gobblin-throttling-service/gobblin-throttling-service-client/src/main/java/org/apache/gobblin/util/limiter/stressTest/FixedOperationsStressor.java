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

package gobblin.util.limiter.stressTest;

import org.apache.hadoop.conf.Configuration;

import gobblin.util.limiter.Limiter;


/**
 * A {@link Stressor} that performs a fixed number of permit requests to the {@link Limiter} without pausing.
 */
public class FixedOperationsStressor extends RandomDelayStartStressor {

  public static final String OPS_TO_RUN = "fixedOperationsStressor.opsToRun";

  public static final int DEFAULT_OPS_TARGET = 200;

  private int opsTarget;

  @Override
  public void configure(Configuration configuration) {
    super.configure(configuration);
    this.opsTarget = configuration.getInt(OPS_TO_RUN, DEFAULT_OPS_TARGET);
  }

  @Override
  public void doRun(Limiter limiter) throws InterruptedException {
    int ops = 0;
    while (ops < this.opsTarget) {
      limiter.acquirePermits(1);
      ops++;
    }
  }
}
