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

package org.apache.gobblin.util.request_allocation;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import lombok.AccessLevel;
import lombok.Getter;

import org.slf4j.Logger;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Iterators;

import org.apache.gobblin.util.Either;
import org.apache.gobblin.util.ExecutorsUtils;
import org.apache.gobblin.util.executors.IteratorExecutor;


public abstract class PriorityIterableBasedRequestAllocator<T extends Request<T>> implements RequestAllocator<T> {

  private final Logger log;
  @Getter(value = AccessLevel.PROTECTED)
  private final RequestAllocatorConfig<T> configuration;

  //These are for submitting alertable events
  @Getter
  private List<T> requestsExceedingAvailableResourcePool;
  @Getter
  private List<T> requestsRejectedWithLowPriority;
  @Getter
  private List<T> requestsRejectedDueToInsufficientEviction;
  @Getter
  private List<T> requestsDropped;

  public PriorityIterableBasedRequestAllocator(Logger log, RequestAllocatorConfig<T> configuration) {
    this.log = log;
    this.configuration = configuration;
  }

  @Override
  public AllocatedRequestsIterator<T> allocateRequests(Iterator<? extends Requestor<T>> requestors,
      ResourcePool resourcePool) {
    final ConcurrentBoundedPriorityIterable<T> iterable =
        new ConcurrentBoundedPriorityIterable<>(this.configuration.getPrioritizer(),
            this.configuration.getResourceEstimator(), this.configuration.getStoreRejectedRequestsSetting(),
            resourcePool);

    final Iterator<T> joinIterator = getJoinIterator(requestors, iterable);

    if (this.configuration.getAllowedThreads() <= 1) {
      while (joinIterator.hasNext()) {
        iterable.add(joinIterator.next());
      }
    } else {

      IteratorExecutor<Void> executor =
          new IteratorExecutor<>(Iterators.transform(joinIterator, new Function<T, Callable<Void>>() {
            @Override
            public Callable<Void> apply(final T input) {
              return new Callable<Void>() {
                @Override
                public Void call()
                    throws Exception {
                  iterable.add(input);
                  return null;
                }
              };
            }
          }), this.configuration.getAllowedThreads(),
              ExecutorsUtils.newThreadFactory(Optional.of(log), Optional.of("request-allocator-%d")));

      try {
        List<Either<Void, ExecutionException>> results = executor.executeAndGetResults();
        // Throw runtime failure if an exception occurs during execution to fail the job
        IteratorExecutor.logAndThrowFailures(results, log, 10);
      } catch (InterruptedException ie) {
        log.error("Request allocation was interrupted.");
        return new AllocatedRequestsIteratorBase<>(
            Iterators.<AllocatedRequestsIteratorBase.RequestWithResourceRequirement<T>>emptyIterator(), resourcePool);
      }
    }

    iterable.logStatistics(Optional.of(this.log));

    //Get all requests rejected/dropped
    getRejectedAndDroppedRequests(iterable);

    return new AllocatedRequestsIteratorBase<>(iterable.iterator(), resourcePool);
  }

  public void getRejectedAndDroppedRequests(ConcurrentBoundedPriorityIterable<T> iterable) {
    requestsExceedingAvailableResourcePool = iterable.getRequestsExceedingAvailableResourcePool();
    requestsRejectedWithLowPriority = iterable.getRequestsRejectedWithLowPriority();
    requestsRejectedDueToInsufficientEviction = iterable.getRequestsRejectedDueToInsufficientEviction();
    requestsDropped = iterable.getRequestsDropped();
  }

  protected abstract Iterator<T> getJoinIterator(Iterator<? extends Requestor<T>> requestors,
      ConcurrentBoundedPriorityIterable<T> requestIterable);
}
