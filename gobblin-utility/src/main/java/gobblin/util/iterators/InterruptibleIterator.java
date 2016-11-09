/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
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
