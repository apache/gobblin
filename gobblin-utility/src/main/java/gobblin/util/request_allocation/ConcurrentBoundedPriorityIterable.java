package gobblin.util.request_allocation;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;

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
 * priority to low priority.
 *
 * Note: as with a priority queue, e1 < e2 means that e1 is higher priority than e2.
 */
@Slf4j
public class ConcurrentBoundedPriorityIterable<T> implements Iterable<AllocatedRequestsBase.TAndRequirement<T>> {

  private final TreeSet<AllocatedRequestsBase.TAndRequirement<T>> elements;
  private final int dimensions;
  @Getter
  private final Comparator<? super T> comparator;
  private final Comparator<AllocatedRequestsBase.TAndRequirement<T>> allDifferentComparator;
  private final ResourceEstimator<T> estimator;
  private final ResourcePool resourcePool;
  private final ResourceRequirement currentRequirement;
  private boolean rejectedElement = false;

  public ConcurrentBoundedPriorityIterable(final Comparator<? super T> prioritizer, ResourceEstimator<T> resourceEstimator, ResourcePool pool) {

    this.estimator = resourceEstimator;
    this.resourcePool = pool;
    this.dimensions = this.resourcePool.getNumDimensions();
    this.comparator = prioritizer;
    this.allDifferentComparator = new AllDifferentComparator();
    this.elements = new TreeSet<>(this.allDifferentComparator);

    this.currentRequirement = this.resourcePool.getResourceRequirementBuilder().zero().build();
  }

  /**
   * This is the actual {@link Comparator} used in the {@link TreeSet}. Since {@link TreeSet}s use the provided
   * {@link Comparator} to determine equality, we must force elements with the same priority to be different according
   * to the {@link TreeSet}.
   */
  private class AllDifferentComparator implements Comparator<AllocatedRequestsBase.TAndRequirement<T>> {
    @Override
    public int compare(AllocatedRequestsBase.TAndRequirement<T> t1, AllocatedRequestsBase.TAndRequirement<T> t2) {
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
    boolean addedWorkunits = addImpl(t);
    if (!addedWorkunits) {
      this.rejectedElement = true;
    }
    return addedWorkunits;
  }

  private synchronized boolean addImpl(T t) {
    AllocatedRequestsBase.TAndRequirement<T> newElement = new AllocatedRequestsBase.TAndRequirement<>(t, this.estimator.estimateRequirement(t, this.resourcePool));

    if (this.resourcePool.exceedsHardBound(newElement.getResourceRequirement(), false)) {
      // item does not fit even in empty pool
      return false;
    }

    ResourceRequirement candidateRequirement = ResourceRequirement.add(this.currentRequirement, newElement.getResourceRequirement(), null);

    if (this.resourcePool.exceedsHardBound(candidateRequirement, false)) {
      if (this.elements.size() == 0) {
        // no elements, first element doesn't fit
        return false;
      }

      if (this.comparator.compare(this.elements.last().getT(), t) <= 0) {
        return false;
      }

      List<AllocatedRequestsBase.TAndRequirement<T>> toDrop = Lists.newArrayList();

      ResourceRequirement tmpRequirement = this.currentRequirement.clone();
      ResourceRequirement reuse = tmpRequirement.clone();

      for (AllocatedRequestsBase.TAndRequirement<T> dropCandidate : this.elements.descendingSet()) {
        if (this.comparator.compare(dropCandidate.getT(), t) <= 0) {
          return false;
        }
        tmpRequirement.subtract(dropCandidate.getResourceRequirement());
        toDrop.add(dropCandidate);
        if (!this.resourcePool.exceedsHardBound(
            ResourceRequirement.add(tmpRequirement, newElement.getResourceRequirement(), reuse), false)) {
          break;
        }
      }

      for (AllocatedRequestsBase.TAndRequirement<T> drop : toDrop) {
        this.currentRequirement.subtract(drop.getResourceRequirement());
        this.elements.remove(drop);
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

  @Override
  public Iterator<AllocatedRequestsBase.TAndRequirement<T>> iterator() {
    return this.elements.iterator();
  }

}
