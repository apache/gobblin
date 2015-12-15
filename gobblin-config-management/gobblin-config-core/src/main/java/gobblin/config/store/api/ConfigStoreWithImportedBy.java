package gobblin.config.store.api;

import java.net.URI;
import java.util.Collection;
import java.util.Map;

import com.typesafe.config.Config;


/**
 * ConfigStoreWithImportedBy indicate this {@ConfigStore} support mapping from the URI to it's imported by URIs
 * @author mitu
 *
 */
public interface ConfigStoreWithImportedBy extends ConfigStore {

  /**
   * @param uri - the uri relative to this configuration store
   * @param version - specify the configuration version in the configuration store.
   * @return - The {@java.util.Collection} of the URI. Each URI in the collection directly import input uri 
   *  against input configuration version
   */
  public Collection<URI> getImportedBy(URI uri, String version) throws VersionDoesNotExistException;

  /**
   * @param uri - the uri relative to this configuration store
   * @param version - specify the configuration version in the configuration store.
   * @return - The {@java.util.Map}. The key of the Map is the URI directly import input uri 
   *  against input configuration version. The value of the Map the directly specified configuration in 
   *  com.typesafe.config.Config format for corresponding key
   */
  public Map<URI, Config> getConfigsImportedBy(URI uri, String version) throws VersionDoesNotExistException;
}
