package gobblin.util.request_allocation;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;

import gobblin.util.ClassAliasResolver;

import lombok.RequiredArgsConstructor;


@RequiredArgsConstructor
public class HierarchicalAllocator<T extends Request<T>> implements RequestAllocator<T> {

  public static class Factory implements RequestAllocator.Factory {
    @Override
    public <T extends Request<T>> RequestAllocator<T> createRequestAllocator(Comparator<T> prioritizer,
        ResourceEstimator<T> resourceEstimator, Config limitedScopeConfig) {
      Preconditions.checkArgument(prioritizer instanceof HierarchicalPrioritizer,
          "Prioritizer must be a " + HierarchicalPrioritizer.class.getSimpleName());
      RequestAllocator<T> underlying = RequestAllocatorUtils.inferFromConfig(prioritizer, resourceEstimator, limitedScopeConfig);
      return new HierarchicalAllocator<>((HierarchicalPrioritizer<T>) prioritizer, underlying);
    }
  }

  private final HierarchicalPrioritizer<T> prioritizer;
  private final RequestAllocator<T> underlying;

  @Override
  public AllocatedRequests<T> allocateRequests(Iterator<? extends Requestor<T>> requestors, ResourcePool resourcePool) {

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

  private class HierarchicalIterator implements AllocatedRequests<T> {
    private SingleTierIterator singleTierIterator;
    private AllocatedRequests<T> currentIterator;
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

    public Optional<SingleTierIterator> nextTier() {
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
