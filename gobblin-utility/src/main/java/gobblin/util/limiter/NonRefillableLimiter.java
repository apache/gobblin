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

package gobblin.util.limiter;

import java.io.Closeable;
import java.io.IOException;


/**
 * A type of {@link Limiter}s that do not support permit refills by returning a no-op
 * {@link Closeable} in {@link #acquirePermits(long)}.
 *
 * @author ynli
 */
public abstract class NonRefillableLimiter implements Limiter {

  protected static final Closeable NO_OP_CLOSEABLE = new Closeable() {
    @Override
    public void close()
        throws IOException {
      // Nothing to do
    }
  };

  @Override
  public Closeable acquirePermits(long permits) throws InterruptedException {
    return NO_OP_CLOSEABLE;
  }
}
