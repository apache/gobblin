package gobblin.config.configstore;

import java.net.URI;
import java.util.Collection;

import com.typesafe.config.Config;

/**
 * ConfigStoreWithResolution is used to indicate the {@ConfigStore} which support the configuration
 * resolution by following the imported path.
 * @author mitu
 *
 */
public interface ConfigStoreWithResolution extends ConfigStore{
  
  /**
   * @param uri - the uri relative to this configuration store
   * @param version - specify the configuration version in the configuration store.
   * @return - the directly and indirectly specified configuration in com.typesafe.config.Config format for input uri 
   *  against input configuration version
   */
  public Config getResolvedConfig(URI uri, String version);
  
  /**
   * @param uri - the uri relative to this configuration store
   * @param version - specify the configuration version in the configuration store.
   * @return - the directly and indirectly imported URIs followed the imported path for input uri 
   *  against input configuration version
   */
  public Collection<URI> getImportsRecursively(URI uri, String version);

}
