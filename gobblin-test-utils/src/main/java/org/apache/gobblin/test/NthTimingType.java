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

/**
 * Every nth call has a different latency than the default latency
 */
public class NthTimingType implements TimingType {
  private final int n;
  private final long defaultTimeMillis;
  private final long nthTimeMillis;
  private int currentNum;

  public NthTimingType(int n, long defaultTimeMillis, long nthTimeMillis) {
    this.n = n;
    this.defaultTimeMillis = defaultTimeMillis;
    this.nthTimeMillis = nthTimeMillis;
    this.currentNum = 0;
  }

  @Override
  public long nextTimeMillis() {
    currentNum++;
    if (currentNum % n == 0) {
      return nthTimeMillis;
    } else {
      return defaultTimeMillis;
    }
  }
}
