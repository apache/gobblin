package gobblin.config.store.api;

import java.net.URI;
import java.util.Collection;
import java.util.Map;

import com.typesafe.config.Config;

/**
 * ConfigStoreWithBatchFetches indicate this {@ConfigStore} support batch fetching for a collection of configUris with
 * specified configuration store version
 * @author mitu
 *
 */
public interface ConfigStoreWithBatchFetches extends ConfigStore {
  /**
   * 
   * @param configUris - the collection of configUris, all the Uris are relative to this configuration store
   * @param version - configuration store version
   * @return the Map whose key is the input configUris, the value is the {@com.typesafe.config.Config} format of 
   *  own configuration for corresponding key
   */
  public Map<URI, Config> getOwnConfigs(Collection<URI> configUris, String version) throws VersionDoesNotExistException;
}
