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

import com.google.common.collect.ImmutableSet;


/**
 * A type of {@link java.lang.Iterable}s corresponding to empty {@link java.util.Collection}s.
 *
 * @author ynli
 *
 * @param <T> record type
 */
public class EmptyIterable<T> implements Iterable<T> {

  @Override
  public Iterator<T> iterator() {
    return ImmutableSet.<T>of().iterator();
  }
}
