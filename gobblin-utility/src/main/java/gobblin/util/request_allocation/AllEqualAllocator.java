package gobblin.util.request_allocation;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.typesafe.config.Config;

import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;


/**
 * A {@link RequestAllocator} optimized for the case when all work units have the same priority. Note the input {@link Comparator}
 * will be ignored.
 */
@Slf4j
@AllArgsConstructor
public class AllEqualAllocator<T extends Request<T>> implements RequestAllocator<T> {

  public static class Factory implements RequestAllocator.Factory {
    @Override
    public <T extends Request<T>> RequestAllocator<T> createRequestAllocator(Comparator<T> prioritizer,
        ResourceEstimator<T> resourceEstimator, Config limitedScopeConfig) {
      return new AllEqualAllocator<>(resourceEstimator);
    }
  }

  private final ResourceEstimator<T> resourceEstimator;

  @Override
  public AllocatedRequests<T> allocateRequests(Iterator<? extends Requestor<T>> requestors, ResourcePool resourcePool) {
    ConcurrentBoundedPriorityIterable<T> iterable =
        new ConcurrentBoundedPriorityIterable<>(new AllEqualComparator(), this.resourceEstimator, resourcePool);

    Iterator<T> unionIterator = Iterators.concat(Iterators.transform(requestors, new Function<Requestor<T>, Iterator<T>>() {
      @Nullable
      @Override
      public Iterator<T> apply(Requestor<T> input) {
        try {
          return input.getRequests();
        } catch (IOException ioe) {
          log.error("Failed to get requests from " + input);
          return Iterators.emptyIterator();
        }
      }
    }));

    while (unionIterator.hasNext() && !iterable.isFull()) {
      iterable.add(unionIterator.next());
    }

    return new AllocatedRequestsBase<>(iterable.iterator(), resourcePool);
  }

  private class AllEqualComparator implements Comparator<T> {
    @Override
    public int compare(T o1, T o2) {
      return 0;
    }
  }

}
