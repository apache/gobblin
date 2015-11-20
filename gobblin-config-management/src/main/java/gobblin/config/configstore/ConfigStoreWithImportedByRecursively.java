package gobblin.config.configstore;

import java.net.URI;
import java.util.Collection;
import java.util.Map;

import com.typesafe.config.Config;

public interface ConfigStoreWithImportedByRecursively extends ConfigStoreWithImportedBy{
  /**
   * @param uri - the uri relative to this configuration store
   * @param version - specify the configuration version in the configuration store.
   * @return - The {@java.util.Collection} of the URI. Each URI in the collection directly or indirectly import input uri 
   *  against input configuration version
   */
  public Collection<URI> getImportedByRecursively(URI uri, String version);
  
  /**
   * @param uri - the uri relative to this configuration store
   * @param version - specify the configuration version in the configuration store.
   * @return - The {@java.util.Map}. The key of the Map is the URI directly or indirectly import input uri 
   *  against input configuration version. The value of the Map the directly or indirectly specified configuration in 
   *  com.typesafe.config.Config format for corresponding key.
   */
  public Map<URI, Config> getConfigsImportedByRecursively(URI uri, String version);
}
