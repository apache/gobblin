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

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;

import com.google.common.collect.Lists;

import gobblin.util.iterators.InterruptibleIterator;

import lombok.extern.slf4j.Slf4j;


/**
 * A {@link RequestAllocator} that operates over {@link PushDownRequestor}s, getting a pre-ordered iterator of
 * {@link Request}s from each {@link Requestor} and merging them, stopping as soon as the {@link ResourcePool} is
 * exhausted.
 *
 * <p>
 *   This {@link RequestAllocator} is ideal when computing the {@link ResourceRequirement} of a {@link Request} is
 *   expensive, as the pre-ordering allows it to not compute {@link ResourceRequirement}s for low priority
 *   {@link Request}s.
 * </p>
 */
@Slf4j
public class PreOrderAllocator<T extends Request<T>> extends PriorityIterableBasedRequestAllocator<T> {

  public static class Factory implements RequestAllocator.Factory {
    @Override
    public <T extends Request<T>> RequestAllocator<T> createRequestAllocator(RequestAllocatorConfig<T> configuration) {
      return new PreOrderAllocator<>(configuration);
    }
  }

  public PreOrderAllocator(RequestAllocatorConfig<T> configuration) {
    super(log, configuration);
  }

  @Override
  protected Iterator<T> getJoinIterator(Iterator<? extends Requestor<T>> requestors,
      final ConcurrentBoundedPriorityIterable<T> requestIterable) {

    List<Iterator<T>> iteratorList = Lists.newArrayList();
    while (requestors.hasNext()) {
      Requestor<T> requestor = requestors.next();
      if (!(requestor instanceof PushDownRequestor)) {
        throw new RuntimeException(String.format("%s can only be used with %s.", PreOrderAllocator.class, PushDownRequestor.class));
      }
      try {
        iteratorList.add(((PushDownRequestor<T>) requestor).getRequests(getConfiguration().getPrioritizer()));
      } catch (IOException ioe) {
        log.error("Failed to get requests from " + requestor);
      }
    }

    PriorityMultiIterator<T> multiIterator = new PriorityMultiIterator<>(iteratorList, getConfiguration().getPrioritizer());

    return new InterruptibleIterator<>(multiIterator, new Callable<Boolean>() {
      @Override
      public Boolean call()
          throws Exception {
        return requestIterable.isFull();
      }
    });
  }
}
