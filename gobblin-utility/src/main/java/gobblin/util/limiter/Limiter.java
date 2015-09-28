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


/**
 * An interface for classes that implement some logic limiting on the occurrences of some events,
 * e.g., data record extraction using an {@link gobblin.source.extractor.Extractor}.
 *
 * @author ynli
 */
public interface Limiter {

  /**
   * Start the {@link Limiter}.
   *
   * See {@link #stop()}
   */
  public void start();

  /**
   * Acquire a given number of permits.
   *
   * <p>
   *   Depending on the implementation, the caller of this method may be blocked.
   *   It is also up to the caller to decide how to deal with the return value.
   * </p>
   *
   * @param permits number of permits to get
   * @return a {@link Closeable} instance if the requested permits have been successfully acquired,
   *         or {@code null} if otherwise; in the former case, calling {@link Closeable#close()} on
   *         the returned {@link Closeable} instance will release the acquired permits.
   * @throws InterruptedException if the caller is interrupted while being blocked
   */
  public Closeable acquirePermits(long permits) throws InterruptedException;

  /**
   * Stop the {@link Limiter}.
   *
   * See {@link #start()}
   */
  public void stop();
}
