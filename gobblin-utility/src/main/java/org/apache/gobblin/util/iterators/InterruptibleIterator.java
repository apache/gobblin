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

package gobblin.util.iterators;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;

import lombok.RequiredArgsConstructor;


/**
 * An iterator that allows ending prematurely (i.e. {@link #hasNext()} becomes false) if a boolean {@link Callable}
 * becomes true.
 */
@RequiredArgsConstructor
public class InterruptibleIterator<T> implements Iterator<T> {

  private final Iterator<T> iterator;
  private final Callable<Boolean> interrupt;

  /** Set to true when user calls {@link #hasNext()} to indicate we can no longer interrupt.*/
  private boolean promisedNext = false;

  @Override
  public boolean hasNext() {
    try {
      if (this.promisedNext || (this.iterator.hasNext() && !this.interrupt.call())) {
        this.promisedNext = true;
        return true;
      }
      return false;
    } catch (Exception exception) {
      throw new RuntimeException(exception);
    }
  }

  @Override
  public T next() {
    if (hasNext()) {
      this.promisedNext = false;
      return this.iterator.next();
    }

    throw new NoSuchElementException();
  }

  @Override
  public void remove() {
    this.iterator.remove();
  }
}
