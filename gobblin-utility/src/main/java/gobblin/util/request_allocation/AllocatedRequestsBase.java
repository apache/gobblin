package gobblin.util.request_allocation;

import java.util.Iterator;
import java.util.Random;

import com.google.common.base.Function;

import lombok.Getter;


/**
 * A basic implementation of {@link AllocatedRequests}.
 * @param <T>
 */
public class AllocatedRequestsBase<T extends Request<T>> implements AllocatedRequests<T> {

  private final Iterator<TAndRequirement<T>> underlying;
  private final double[] currentRequirement;

  public AllocatedRequestsBase(Iterator<TAndRequirement<T>> underlying, ResourcePool resourcePool) {
    this.underlying = underlying;
    this.currentRequirement = new double[resourcePool.getNumDimensions()];
  }

  @Override
  public ResourceRequirement totalResourcesUsed() {
    return new ResourceRequirement(this.currentRequirement);
  }

  @Override
  public boolean hasNext() {
    return this.underlying.hasNext();
  }

  @Override
  public T next() {
    TAndRequirement<T> nextElement = this.underlying.next();
    VectorAlgebra.addVector(this.currentRequirement, nextElement.getResourceRequirement().getResourceVector(), 1.0,
        this.currentRequirement);
    return nextElement.getT();
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  /**
   * Stores and element and its {@link ResourceRequirement}.
   */
  @Getter
  public static class TAndRequirement<T> {
    public static final Random RANDOM = new Random();

    private final T t;
    private final ResourceRequirement resourceRequirement;
    private final long id;

    TAndRequirement(T t, ResourceRequirement resourceRequirement) {
      this.t = t;
      this.resourceRequirement = resourceRequirement;
      this.id = RANDOM.nextLong();
    }
  }

  /**
   * A {@link Function} used to extract the actual {@link Request} from a {@link TAndRequirement}.
   */
  public static class TExtractor<T> implements Function<TAndRequirement<T>, T> {
    @Override
    public T apply(TAndRequirement<T> ttAndRequirement) {
      return ttAndRequirement.getT();
    }
  }
}
