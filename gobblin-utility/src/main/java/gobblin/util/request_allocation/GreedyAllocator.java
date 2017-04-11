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
import java.util.concurrent.Callable;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;

import gobblin.util.iterators.InterruptibleIterator;

import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;


/**
 * A {@link RequestAllocator} that selects {@link Request}s without any order guarantees until the {@link ResourcePool}
 * is full, then stops. This allocator will mostly ignore the prioritizer.
 *
 * <p>
 *   This allocator is useful when there is no prioritization or fairness required. It is generally the fastest and least
 *   memory intensive implementation.
 * </p>
 */
@Slf4j
public class GreedyAllocator<T extends Request<T>> extends PriorityIterableBasedRequestAllocator<T> {

  public static class Factory implements RequestAllocator.Factory {
    @Override
    public <T extends Request<T>> RequestAllocator<T> createRequestAllocator(RequestAllocatorConfig<T> configuration) {
      return new GreedyAllocator<>(configuration);
    }
  }

  public GreedyAllocator(RequestAllocatorConfig<T> configuration) {
    super(log, configuration);
  }

  @Override
  protected Iterator<T> getJoinIterator(Iterator<? extends Requestor<T>> requestors,
      final ConcurrentBoundedPriorityIterable<T> requestIterable) {
    Iterator<T> unionIterator = Iterators.concat(Iterators.transform(requestors, new Function<Requestor<T>, Iterator<T>>() {
      @Nullable
      @Override
      public Iterator<T> apply(Requestor<T> input) {
        return input.iterator();
      }
    }));

    return new InterruptibleIterator<>(unionIterator, new Callable<Boolean>() {
      @Override
      public Boolean call()
          throws Exception {
        return requestIterable.isFull();
      }
    });
  }
}
