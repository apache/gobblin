package gobblin.util.request_allocation;

import com.typesafe.config.Config;


/**
 * Computes the {@link ResourceRequirement} for {@link Request}s. See {@link RequestAllocator}.
 * @param <T>
 */
public interface ResourceEstimator<T> {

  interface Factory<T> {
    ResourceEstimator<T> create(Config config);
  }

  /**
   * @return The {@link ResourceRequirement} for input {@link Request}.
   */
  ResourceRequirement estimateRequirement(T t, ResourcePool resourcePool);
}
