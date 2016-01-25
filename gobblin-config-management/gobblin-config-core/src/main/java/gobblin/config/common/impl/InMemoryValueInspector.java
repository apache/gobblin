package gobblin.config.common.impl;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.typesafe.config.Config;

import gobblin.config.store.api.ConfigKeyPath;
import gobblin.config.store.api.ConfigStore;

/**
 * InMemoryValueInspector provide the caching layer for getting the {@link com.typesafe.config.Config} from {@link ConfigStore}
 * 
 * @author mitu
 *
 */
public class InMemoryValueInspector implements ConfigStoreValueInspector{

  private final ConfigStoreValueInspector valueFallback;
  private final Cache<ConfigKeyPath, Config> ownConfigCache ;
  private final Cache<ConfigKeyPath, Config> recursiveConfigCache ;

  /**
   * 
   * @param valueFallback - the fall back {@link ConfigStoreValueInspector} which used to get the raw {@link com.typesafe.config.Config} 
   * @param useStrongRef  - if true, use Strong reference in cache, else, use Weak reference in cache
   */
  public InMemoryValueInspector (ConfigStoreValueInspector valueFallback, boolean useStrongRef){
    this.valueFallback = valueFallback;

    if (useStrongRef) {
      ownConfigCache = CacheBuilder.newBuilder().build();
      recursiveConfigCache = CacheBuilder.newBuilder().build();
    }
    else{
      ownConfigCache = CacheBuilder.newBuilder().softValues().build();
      recursiveConfigCache = CacheBuilder.newBuilder().softValues().build();
    }
  }

  /**
   * {@inheritDoc}.
   *
   * <p>
   *   If present in the cache, return the cached {@link com.typesafe.config.Config} for given input
   *   Otherwise, simply delegate the functionality to the internal {ConfigStoreValueInspector} and store the value into cache
   * </p>
   */
  @Override
  public Config getOwnConfig(final ConfigKeyPath configKey) {
    try {
      return this.ownConfigCache.get(configKey, new Callable<Config>() {
        @Override
        public Config call()  {
          return valueFallback.getOwnConfig(configKey);
        }
      });
    } catch (ExecutionException e) {
      // should NOT come here
      throw new RuntimeException("Can not getOwnConfig for " + configKey);
    }
  }

  /**
   * {@inheritDoc}.
   *
   * <p>
   *   If present in the cache, return the cached {@link com.typesafe.config.Config} for given input
   *   Otherwise, simply delegate the functionality to the internal {ConfigStoreValueInspector} and store the value into cache
   * </p>
   */
  @Override
  public Config getResolvedConfig(final ConfigKeyPath configKey) {
    try {
      return this.recursiveConfigCache.get(configKey, new Callable<Config>() {
        @Override
        public Config call()  {
          return valueFallback.getResolvedConfig(configKey);
        }
      });
    } catch (ExecutionException e) {
      // should NOT come here
      throw new RuntimeException("Can not getOwnConfig for " + configKey);
    }
  }
}
