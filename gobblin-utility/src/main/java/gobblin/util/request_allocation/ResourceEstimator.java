package gobblin.util.request_allocation;

import com.typesafe.config.Config;


public interface ResourceEstimator<T> {

  interface Factory<T> {
    ResourceEstimator<T> create(Config config);
  }

  ResourceRequirement estimateRequirement(T t, ResourcePool resourcePool);
}
