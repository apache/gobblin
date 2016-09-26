package gobblin.util.request_allocation;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Lists;
import com.typesafe.config.Config;

import lombok.AllArgsConstructor;
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
@AllArgsConstructor
@Slf4j
public class PreOrderAllocator<T extends Request<T>> implements RequestAllocator<T> {

  public static class Factory implements RequestAllocator.Factory {
    @Override
    public <T extends Request<T>> RequestAllocator<T> createRequestAllocator(Comparator<T> prioritizer,
        ResourceEstimator<T> resourceEstimator, Config limitedScopeConfig) {
      return new PreOrderAllocator<>(prioritizer, resourceEstimator);
    }
  }

  private final Comparator<T> prioritizer;
  private final ResourceEstimator<T> resourceEstimator;

  @Override
  public AllocatedRequests<T> allocateRequests(Iterator<? extends Requestor<T>> requestors, ResourcePool resourcePool) {

    List<Iterator<T>> iteratorList = Lists.newArrayList();
    while (requestors.hasNext()) {
      Requestor<T> requestor = requestors.next();
      if (!(requestor instanceof PushDownRequestor)) {
        throw new RuntimeException(String.format("%s can only be used with %s.", PreOrderAllocator.class, PushDownRequestor.class));
      }
      try {
        iteratorList.add(((PushDownRequestor<T>) requestor).getRequests(this.prioritizer));
      } catch (IOException ioe) {
        log.error("Failed to get requests from " + requestor);
      }
    }

    PriorityMultiIterator<T> multiIterator = new PriorityMultiIterator<>(iteratorList, this.prioritizer);

    ConcurrentBoundedPriorityIterable<T> iterable =
        new ConcurrentBoundedPriorityIterable<>(this.prioritizer, this.resourceEstimator, resourcePool);
    while (multiIterator.hasNext() && !iterable.isFull()) {
      iterable.add(multiIterator.next());
    }

    return new AllocatedRequestsBase<>(iterable.iterator(), resourcePool);
  }
}
