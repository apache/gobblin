package gobblin.util.request_allocation;

import java.util.Comparator;

import com.typesafe.config.Config;

import gobblin.util.ClassAliasResolver;


public class RequestAllocatorUtils {

  public static final String ALLOCATOR_ALIAS_KEY = "requestAllocatorAlias";
  public static <T extends Request<T>> RequestAllocator<T> inferFromConfig(Comparator<T> prioritizer, ResourceEstimator<T> resourceEstimator,
      Config limitedScopeConfig) {
    try {
      String alias = limitedScopeConfig.hasPath(ALLOCATOR_ALIAS_KEY) ? limitedScopeConfig.getString(ALLOCATOR_ALIAS_KEY) :
          BruteForceAllocator.Factory.class.getName();
      RequestAllocator.Factory allocatorFactory = new ClassAliasResolver<>(RequestAllocator.Factory.class).
          resolveClass(alias).newInstance();
      return allocatorFactory.createRequestAllocator(prioritizer, resourceEstimator, limitedScopeConfig);
    } catch (ReflectiveOperationException roe) {
      throw new RuntimeException(roe);
    }
  }

}
