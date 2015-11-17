package gobblin.config.configstore;

import java.net.URI;

/**
 * ConfigStoreFactory is used to created {@ConfigStore}s. 
 * @author mitu
 *
 * @param <T> The java class which extends {@ConfigStore}
 */
public interface ConfigStoreFactory<T extends ConfigStore> {

  /**
   * @return the scheme of this configuration store factory. All the configuration store created by 
   * this configuration factory will share the same scheme name.
   */
  public String getScheme();

  /**
   * 
   * @return the default {@ConfigStore}
   */
  public T getDefaultConfigStore();

  /**
   * @param uri - The specified URI in the configuration store
   * @return {@ConfigStore} which is created from the input URI
   */
  public T createConfigStore(URI uri);
}
