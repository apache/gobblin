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

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;


/**
 * A {@link RequestAllocator} optimized for {@link HierarchicalPrioritizer}s. Processes {@link Requestor}s in order of
 * priority. Once the {@link ResourcePool} is full, lower priority {@link Requestor}s will not even be materialized.
 */
@RequiredArgsConstructor
@Slf4j
public class HierarchicalAllocator<T extends Request<T>> implements RequestAllocator<T> {

  public static class Factory implements RequestAllocator.Factory {
    @Override
    public <T extends Request<T>> RequestAllocator<T> createRequestAllocator(RequestAllocatorConfig<T> cofiguration) {
      Preconditions.checkArgument(cofiguration.getPrioritizer() instanceof HierarchicalPrioritizer,
          "Prioritizer must be a " + HierarchicalPrioritizer.class.getSimpleName());
      RequestAllocator<T> underlying = RequestAllocatorUtils.inferFromConfig(cofiguration);
      return new HierarchicalAllocator<>((HierarchicalPrioritizer<T>) cofiguration.getPrioritizer(), underlying);
    }
  }

  private final HierarchicalPrioritizer<T> prioritizer;
  private final RequestAllocator<T> underlying;

  @Override
  public AllocatedRequestsIterator<T> allocateRequests(Iterator<? extends Requestor<T>> requestors, ResourcePool resourcePool) {

    List<Requestor<T>> requestorList = Lists.newArrayList(requestors);

    Comparator<Requestor<T>> requestorComparator = new Comparator<Requestor<T>>() {
      @Override
      public int compare(Requestor<T> o1, Requestor<T> o2) {
        return prioritizer.compareRequestors(o1, o2);
      }
    };
    Collections.sort(requestorList, requestorComparator);

    return new HierarchicalIterator(resourcePool, new SingleTierIterator(requestorComparator, requestorList));
  }

  /**
   * Automatically handles allocation for each tier of {@link Requestor}s, computation of correct {@link #totalResourcesUsed()},
   * and not materializing next tier once {@link ResourcePool} is full.
   */
  private class HierarchicalIterator implements AllocatedRequestsIterator<T> {
    private SingleTierIterator singleTierIterator;
    private AllocatedRequestsIterator<T> currentIterator;
    private ResourcePool resourcePool;
    private final ResourceRequirement currentRequirement;

    public HierarchicalIterator(ResourcePool resourcePool, SingleTierIterator singleTierIterator) {
      this.singleTierIterator = singleTierIterator;
      this.resourcePool = resourcePool;
      this.currentRequirement = resourcePool.getResourceRequirementBuilder().zero().build();
    }

    @Override
    public boolean hasNext() {
      while (this.currentIterator == null || !this.currentIterator.hasNext()) {
        if (this.currentIterator != null) {
          this.currentRequirement.add(this.currentIterator.totalResourcesUsed());
        }

        if (this.resourcePool.exceedsSoftBound(this.currentRequirement, true)) {
          return false;
        }

        Optional<SingleTierIterator> tmp = this.singleTierIterator.nextTier();
        if (!tmp.isPresent()) {
          return false;
        }
        this.singleTierIterator = tmp.get();
        ResourcePool contractedPool = this.resourcePool.contractPool(this.currentRequirement);
        this.currentIterator = HierarchicalAllocator.this.underlying.allocateRequests(this.singleTierIterator, contractedPool);
      }
      return true;
    }

    @Override
    public T next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      return this.currentIterator.next();
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

    @Override
    public ResourceRequirement totalResourcesUsed() {
      return ResourceRequirement.add(this.currentRequirement, this.currentIterator.totalResourcesUsed(), null);
    }
  }

  /**
   * An {@link Iterator} that only returns entries of the same priority as the first entry it returns. Assumes the input
   * {@link List} is sorted.
   */
  private class SingleTierIterator implements Iterator<Requestor<T>> {
    private final Comparator<Requestor<T>> prioritizer;
    private final List<Requestor<T>> requestors;
    private final Requestor<T> referenceRequestor;
    private int nextRequestorIdx;

    public SingleTierIterator(Comparator<Requestor<T>> prioritizer, List<Requestor<T>> requestors) {
      this(prioritizer, requestors, 0);
    }

    private SingleTierIterator(Comparator<Requestor<T>> prioritizer, List<Requestor<T>> requestors, int initialIndex) {
      this.prioritizer = prioritizer;
      this.requestors = requestors;
      if (this.requestors.size() > initialIndex) {
        this.referenceRequestor = requestors.get(initialIndex);
      } else {
        this.referenceRequestor = null;
      }
      this.nextRequestorIdx = initialIndex;
      log.debug("Starting a single tier iterator with reference requestor: {}", this.referenceRequestor);
    }

    @Override
    public boolean hasNext() {
      return this.requestors.size() > nextRequestorIdx &&
          this.prioritizer.compare(this.referenceRequestor, this.requestors.get(this.nextRequestorIdx)) == 0;
    }

    @Override
    public Requestor<T> next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      return this.requestors.get(this.nextRequestorIdx++);
    }

    /**
     * @return a {@link SingleTierIterator} for the next tier in the input {@link List}.
     */
    Optional<SingleTierIterator> nextTier() {
      if (this.nextRequestorIdx < this.requestors.size()) {
        return Optional.of(new SingleTierIterator(this.prioritizer, this.requestors, this.nextRequestorIdx));
      }
      return Optional.absent();
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
