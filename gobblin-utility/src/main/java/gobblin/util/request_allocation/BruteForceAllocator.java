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
 * A {@link RequestAllocator} that simply adds every work unit to a {@link ConcurrentBoundedPriorityIterable}, then returns
 * the iterator.
 */
@Slf4j
@AllArgsConstructor
public class BruteForceAllocator<T extends Request<T>> implements RequestAllocator<T> {

  public static class Factory implements RequestAllocator.Factory {
    @Override
    public <T extends Request<T>> RequestAllocator<T> createRequestAllocator(Comparator<T> prioritizer,
        ResourceEstimator<T> resourceEstimator, Config limitedScopeConfig) {
      return new BruteForceAllocator<>(prioritizer, resourceEstimator);
    }
  }

  private final Comparator<T> comparator;
  private final ResourceEstimator<T> resourceEstimator;

  @Override
  public AllocatedRequests<T> allocateRequests(Iterator<? extends Requestor<T>> requestors, ResourcePool resourcePool) {
    ConcurrentBoundedPriorityIterable<T> iterable =
        new ConcurrentBoundedPriorityIterable<>(this.comparator, this.resourceEstimator, resourcePool);

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

    while (unionIterator.hasNext()) {
      iterable.add(unionIterator.next());
    }

    return new AllocatedRequestsBase<>(iterable.iterator(), resourcePool);
  }

}
