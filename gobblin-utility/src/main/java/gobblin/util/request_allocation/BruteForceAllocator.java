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

package gobblin.util.request_allocation;

import java.util.Iterator;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;

import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;


/**
 * A {@link RequestAllocator} that simply adds every work unit to a {@link ConcurrentBoundedPriorityIterable}, then returns
 * the iterator.
 */
@Slf4j
public class BruteForceAllocator<T extends Request<T>> extends PriorityIterableBasedRequestAllocator<T> {

  public static class Factory implements RequestAllocator.Factory {
    @Override
    public <T extends Request<T>> RequestAllocator<T> createRequestAllocator(RequestAllocatorConfig<T> configuration) {
      return new BruteForceAllocator<>(configuration);
    }
  }

  public BruteForceAllocator(RequestAllocatorConfig<T> configuration) {
    super(log, configuration);
  }

  @Override
  protected Iterator<T> getJoinIterator(Iterator<? extends Requestor<T>> requestors,
      ConcurrentBoundedPriorityIterable<T> requestIterable) {
    return Iterators.concat(Iterators.transform(requestors, new Function<Requestor<T>, Iterator<T>>() {
      @Nullable
      @Override
      public Iterator<T> apply(Requestor<T> input) {
        return input.iterator();
      }
    }));
  }
}
