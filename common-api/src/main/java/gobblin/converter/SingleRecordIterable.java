/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.converter;

import java.util.Iterator;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;


/**
 * A type of {@link Iterable}s for a single non-nullable record.
 *
 * @author ynli
 *
 * @param <T> record type
 */
public class SingleRecordIterable<T> implements Iterable<T> {

  private final T value;

  public SingleRecordIterable(T value) {
    Preconditions.checkNotNull(value);
    this.value = value;
  }

  @Override
  public Iterator<T> iterator() {
    return Iterators.singletonIterator(this.value);
  }
}
