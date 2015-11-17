package gobblin.config.client;

import java.net.URI;
import java.util.Collection;
import java.util.Map;

import com.typesafe.config.Config;

import gobblin.config.configstore.ConfigStore;

/**
 * ConfigClient is the consumer facing class for retrieving configuration against all configuration stores.
 * @author mitu
 *
 */
public class ConfigClient {

  /**
   * RAISE_ERROR 
   * @author mitu
   *
   */
  public static enum VERSION_STABILITY_POLICY{
    RAISE_ERROR,
    CACHE_CONFIG_IN_MEMORY
  }
  
  /**
   * @param policy - the policy control the behavior when this configuration client try to connect to configuration store.
   * if policy is RAISE_ERROR and this client try to connect to any {@gobblin.config.configstore.ConfigStore} which is not
   * {@gobblin.config.configstore.ConfigStoreWithStableVersion}, the RuntimeException will be thrown
   * 
   * if policy is CACHE_CONFIG_IN_MEMORY and this client try to connect to any {@gobblin.config.configstore.ConfigStore} which is not
   * {@gobblin.config.configstore.ConfigStoreWithStableVersion}, the consumer need to cache the retrieved configuration in memory,
   * otherwise, the same query for the same URI against same configuration store may result different result.
   * @return - the Configuration Client object
   */
  public static ConfigClient createConfigClientWithStableVersion( VERSION_STABILITY_POLICY policy ){
    return null;
  }
  
  /**
   * 
   * @return the {@ConfigClient} with VERSION_STABILITY_POLICY.CACHE_CONFIG_IN_MEMORY
   */
  public static ConfigClient createDefaultConfigClient( ){
    // create without stable versions
    return createConfigClientWithStableVersion(VERSION_STABILITY_POLICY.CACHE_CONFIG_IN_MEMORY);
  }
  
  // public APIs
  /**
   * 
   * @param uri - must start with scheme name
   * @return - the directly and indirectly specified configuration in {@com.typesafe.config.Config} format for input uri 
   * 
   * <p>
   * detail logics:
   * 0. Used the ConfigStoreAccessor in the cached map if previous ConfigStore been queried
   * 1. Based the scheme name, using {@java.util.ServiceLoader} to find the first {@gobblin.config.configstore.ConfigStoreFactory}
   * 2. Use ConfigStoreFactory to create ConfigStore 
   *    using getDefaultConfigStore() if Authority is missing in uri
   *    using createConfigStore() if Authority is present
   * 3. Find ConfigStore's root URI by back tracing the input uri, the path which contains "_CONFIG_STORE" is the root
   * 4. Build ConfigStoreAccessor by checking the current version of the ConfigStore. Added the entry to theMap
   * 
   * 5. If the ConfigStore is NOT ConfigStoreWithResolution, need to do resolution in this client
   */
  public Config getConfig(URI uri){
    
    // from scheme, get all subclass of ConfigStoreFactory, match it's scheme name to get ConfigStoreFactory
//    ConfigStore cs = ConfigStoreFactory.getDefaultConfigStore();
//    theMap.put(cs.getServerURI, new ConfigStoreAccessor(cs.getCurrentVersion, cs));
//    cs.getConfig(uri.getRelativePath());
    return null;
  }
  
  /**
   * @param uris - Collection of URI, each one much start with scheme name
   * @return - the java.util.Map. Key of the map is the URI, value of the Map is getConfig(URI key)
   */
  public Map<URI, Config> getConfigs(Collection<URI> uris){
    return null;
  }
  
  /**
   * 
   * @param uri - URI which must start with scheme name
   * @param recursive - indicate to get the imported URI recursively or not
   * @return The java.util.Collection which contains all the URI imported by input uri
   */
  public Collection<URI> getImported(URI uri, boolean recursive){
    return null;
  }
  
  /**
   * 
   * @param uri - URI which must start with scheme name
   * @param recursive - indicate to get the imported by URI recursively or not
   * @return The java.util.Collection which contains all the URI imported by with input uri
   */
  public Collection<URI> getImportedBy(URI uri, boolean recursive){
    return null;
  }
  
  /**
   * 
   * @param uri - clean the cache for the configuration store which specified by input URI 
   */
  public void clearCache(URI uri){
    
  }
  
  // key is the store ROOT, must use TreeMap
  Map<URI, ConfigStoreAccessor> theMap;

  
  static class ConfigStoreAccessor{
    String version;
    ConfigStore store;
  }
}
