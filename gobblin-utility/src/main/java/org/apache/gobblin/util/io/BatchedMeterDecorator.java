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

package org.apache.gobblin.util.io;

import com.codahale.metrics.Meter;

import org.apache.gobblin.util.Decorator;

import javax.annotation.concurrent.NotThreadSafe;


/**
 * A decorator to a {@link Meter} that batches updates for performance.
 */
@NotThreadSafe
public class BatchedMeterDecorator implements Decorator {

  private final Meter underlying;
  private final int updateFrequency;
  private int count;

  public BatchedMeterDecorator(Meter underlying, int updateFrequency) {
    this.underlying = underlying;
    this.updateFrequency = updateFrequency;
    this.count = 0;
  }

  public void mark() {
    this.count++;
    if (this.count > this.updateFrequency) {
      updateUnderlying();
    }
  }

  public void mark(long n) {
    this.count += n;
    if (this.count > this.updateFrequency) {
      updateUnderlying();
    }
  }

  private void updateUnderlying() {
    this.underlying.mark(this.count);
    this.count = 0;
  }

  @Override
  public Object getDecoratedObject() {
    return this.underlying;
  }

  public Meter getUnderlyingMeter() {
    return this.underlying;
  }
}
