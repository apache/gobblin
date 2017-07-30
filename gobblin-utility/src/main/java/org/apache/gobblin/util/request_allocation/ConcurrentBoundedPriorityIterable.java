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

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

import org.slf4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;


/**
 * A concurrent bounded priority {@link Iterable}. Given a {@link ResourcePool}, a {@link ResourceEstimator}, and a
 * {@link Comparator} as a prioritizer, user can attempt to add elements to this container.
 * The container stores a subset of the offered elements such that their {@link ResourceRequirement} is within the
 * bounds of the {@link ResourcePool}, preferentially storing high priority elements.
 * This functionality is achieved by automatic eviction of low priority items when space for a higher priority item
 * is needed.
 * A call to {@link #iterator()} returns an iterator over the current elements in the container in order from high
 * priority to low priority. Note after calling {@link #iterator()}, no more requests can be added (will throw
 * {@link RuntimeException}).
 *
 * Note: as with a priority queue, e1 < e2 means that e1 is higher priority than e2.
 */
@Slf4j
public class ConcurrentBoundedPriorityIterable<T> implements Iterable<AllocatedRequestsIteratorBase.RequestWithResourceRequirement<T>> {

  private final TreeSet<AllocatedRequestsIteratorBase.RequestWithResourceRequirement<T>> elements;
  private final int dimensions;
  @Getter
  private final Comparator<? super T> comparator;
  private final Comparator<AllocatedRequestsIteratorBase.RequestWithResourceRequirement<T>> allDifferentComparator;
  private final ResourceEstimator<T> estimator;
  private final ResourcePool resourcePool;
  private final ResourceRequirement currentRequirement;
  private volatile boolean rejectedElement = false;
  private volatile boolean closed = false;

  // These are just for statistics
  private final ResourceRequirement maxResourceRequirement;
  private int requestsOffered = 0;
  private int requestsRefused = 0;
  private int requestsEvicted = 0;

  // These are ResourceRequirements for temporary use to avoid instantiation costs
  private final ResourceRequirement candidateRequirement;
  private final ResourceRequirement tmpRequirement;
  private final ResourceRequirement reuse;

  public ConcurrentBoundedPriorityIterable(final Comparator<? super T> prioritizer, ResourceEstimator<T> resourceEstimator, ResourcePool pool) {

    this.estimator = resourceEstimator;
    this.resourcePool = pool;
    this.dimensions = this.resourcePool.getNumDimensions();
    this.comparator = prioritizer;
    this.allDifferentComparator = new AllDifferentComparator();
    this.elements = new TreeSet<>(this.allDifferentComparator);

    this.currentRequirement = this.resourcePool.getResourceRequirementBuilder().zero().build();
    this.maxResourceRequirement = new ResourceRequirement(this.currentRequirement);

    this.candidateRequirement = new ResourceRequirement(this.currentRequirement);
    this.tmpRequirement = new ResourceRequirement(this.currentRequirement);
    this.reuse = new ResourceRequirement(this.currentRequirement);
  }

  /**
   * This is the actual {@link Comparator} used in the {@link TreeSet}. Since {@link TreeSet}s use the provided
   * {@link Comparator} to determine equality, we must force elements with the same priority to be different according
   * to the {@link TreeSet}.
   */
  private class AllDifferentComparator implements Comparator<AllocatedRequestsIteratorBase.RequestWithResourceRequirement<T>> {
    @Override
    public int compare(AllocatedRequestsIteratorBase.RequestWithResourceRequirement<T> t1, AllocatedRequestsIteratorBase.RequestWithResourceRequirement<T> t2) {
      int providedComparison = ConcurrentBoundedPriorityIterable.this.comparator.compare(t1.getT(), t2.getT());
      if (providedComparison != 0) {
        return providedComparison;
      }
      return Long.compare(t1.getId(), t2.getId());
    }
  }

  /**
   * Offer an element to the container.
   * @return true if the element was added, false if there was no space and we could not evict any elements to make it fit.
   *         Note that the element may get evicted by future offers, so a return of true is not a guarantee that the
   *         element will be present at any time in the future.
   */
  public boolean add(T t) {
    if (this.closed) {
      throw new RuntimeException(ConcurrentBoundedPriorityIterable.class.getSimpleName() + " is no longer accepting requests!");
    }

    AllocatedRequestsIteratorBase.RequestWithResourceRequirement<T>
        newElement = new AllocatedRequestsIteratorBase.RequestWithResourceRequirement<>(t,
        this.estimator.estimateRequirement(t, this.resourcePool));
    boolean addedWorkunits = addImpl(newElement);
    if (!addedWorkunits) {
      this.rejectedElement = true;
    }
    return addedWorkunits;
  }

  private synchronized boolean addImpl(AllocatedRequestsIteratorBase.RequestWithResourceRequirement<T> newElement) {

    this.maxResourceRequirement.entryWiseMax(newElement.getResourceRequirement());
    this.requestsOffered++;

    if (this.resourcePool.exceedsHardBound(newElement.getResourceRequirement(), false)) {
      // item does not fit even in empty pool
      log.warn(String.format("Request %s is larger than the available resource pool. If the pool is not expanded, "
          + "it will never be selected. Request: %s.", newElement.getT(),
          this.resourcePool.stringifyRequirement(newElement.getResourceRequirement())));
      this.requestsRefused++;
      return false;
    }

    ResourceRequirement candidateRequirement =
        ResourceRequirement.add(this.currentRequirement, newElement.getResourceRequirement(), this.candidateRequirement);

    if (this.resourcePool.exceedsHardBound(candidateRequirement, false)) {

      if (this.comparator.compare(this.elements.last().getT(), newElement.getT()) <= 0) {
        log.debug("Request {} does not fit in resource pool and is lower priority than current lowest priority request. "
            + "Rejecting", newElement.getT());
        this.requestsRefused++;
        return false;
      }

      List<AllocatedRequestsIteratorBase.RequestWithResourceRequirement<T>> toDrop = Lists.newArrayList();

      this.currentRequirement.copyInto(this.tmpRequirement);

      for (AllocatedRequestsIteratorBase.RequestWithResourceRequirement<T> dropCandidate : this.elements.descendingSet()) {
        if (this.comparator.compare(dropCandidate.getT(), newElement.getT()) <= 0) {
          log.debug("Cannot evict enough requests to fit request {}. "
              + "Rejecting", newElement.getT());
          this.requestsRefused++;
          return false;
        }
        this.tmpRequirement.subtract(dropCandidate.getResourceRequirement());
        toDrop.add(dropCandidate);
        if (!this.resourcePool.exceedsHardBound(
            ResourceRequirement.add(this.tmpRequirement, newElement.getResourceRequirement(), this.reuse), false)) {
          break;
        }
      }

      for (AllocatedRequestsIteratorBase.RequestWithResourceRequirement<T> drop : toDrop) {
        log.debug("Evicting request {}.", drop.getT());
        this.requestsEvicted++;
        this.elements.remove(drop);
        this.currentRequirement.subtract(drop.getResourceRequirement());
      }
    }

    this.elements.add(newElement);
    this.currentRequirement.add(newElement.getResourceRequirement());
    return true;
  }

  /**
   * @return Whether any calls to {@link #add} have returned false, i.e. some element has been rejected due
   * to strict capacity issues.
   */
  public boolean hasRejectedElement() {
    return this.rejectedElement;
  }

  /**
   * @return Whether the list has reached its soft bound.
   */
  public synchronized boolean isFull() {
    return this.resourcePool.exceedsSoftBound(this.currentRequirement, true);
  }

  /**
   * Log statistics about this {@link ConcurrentBoundedPriorityIterable}.
   */
  public synchronized void logStatistics(Optional<Logger> logger) {
    Logger actualLogger = logger.or(log);
    StringBuilder messageBuilder = new StringBuilder("Statistics for ").
        append(ConcurrentBoundedPriorityIterable.class.getSimpleName()).append(": {");
    messageBuilder.append(this.resourcePool).append(", ");
    messageBuilder.append("totalResourcesUsed: ").append(this.resourcePool.stringifyRequirement(this.currentRequirement))
        .append(", ");
    messageBuilder.append("maxRequirementPerDimension: ").append(this.resourcePool.stringifyRequirement(this.maxResourceRequirement))
        .append(", ");
    messageBuilder.append("requestsOffered: ").append(this.requestsOffered).append(", ");
    messageBuilder.append("requestsAccepted: ").append(this.requestsOffered - this.requestsEvicted - this.requestsRefused)
        .append(", ");
    messageBuilder.append("requestsRefused: ").append(this.requestsRefused).append(", ");
    messageBuilder.append("requestsEvicted: ").append(this.requestsEvicted);
    messageBuilder.append("}");
    actualLogger.info(messageBuilder.toString());
  }

  @VisibleForTesting
  void reopen() {
    this.closed = false;
  }

  @Override
  public Iterator<AllocatedRequestsIteratorBase.RequestWithResourceRequirement<T>> iterator() {
    this.closed = true;
    return this.elements.iterator();
  }

}
