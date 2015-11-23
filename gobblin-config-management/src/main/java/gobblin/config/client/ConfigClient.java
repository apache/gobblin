package gobblin.config.client;

import gobblin.config.configstore.ConfigStore;
import gobblin.config.configstore.ConfigStoreFactory;
import gobblin.config.configstore.ConfigStoreWithImportedBy;
import gobblin.config.configstore.ConfigStoreWithImportedByRecursively;
import gobblin.config.configstore.ConfigStoreWithResolution;
import gobblin.config.configstore.ConfigStoreWithStableVersion;
import gobblin.config.configstore.impl.ETLHdfsConfigStoreFactory;
import gobblin.config.configstore.impl.SimpleConfigStoreResolver;
import gobblin.config.configstore.impl.SimpleImportMappings;
import gobblin.config.utils.URIComparator;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.typesafe.config.Config;


/**
 * ConfigClient is the consumer facing class for retrieving configuration against all configuration stores.
 * @author mitu
 *
 */
public class ConfigClient {

  public static enum VERSION_STABILITY_POLICY {
    RAISE_ERROR,
    CACHE_CONFIG_IN_MEMORY
  }

  private final VERSION_STABILITY_POLICY policy;

  // key is the store ROOT, must use TreeMap
  private final TreeMap<URI, ConfigStoreAccessor> configStoreMap = new TreeMap<URI, ConfigStoreAccessor>(
      new URIComparator());

  private ConfigClient(VERSION_STABILITY_POLICY policy) {
    this.policy = policy;
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
  public static ConfigClient createConfigClientWithStableVersion(VERSION_STABILITY_POLICY policy) {
    return new ConfigClient(policy);
  }

  /**
   * 
   * @return the {@ConfigClient} with policy set to VERSION_STABILITY_POLICY.RAISE_ERROR
   */
  public static ConfigClient createDefaultConfigClient() {
    // create with stable versions
    return createConfigClientWithStableVersion(VERSION_STABILITY_POLICY.RAISE_ERROR);
  }

  private ConfigStoreAccessor getConfigStoreAccessor(URI uri) throws Exception {
    if (this.configStoreMap.containsKey(uri)) {
      return this.configStoreMap.get(uri);
    }

    ConfigStoreFactory<ConfigStore> csFactory = this.getConfigStoreFactory(uri);
    ConfigStore cs = csFactory.createConfigStore(uri);
    
    if(!(cs instanceof ConfigStoreWithStableVersion)){
      if(this.policy == VERSION_STABILITY_POLICY.RAISE_ERROR){
        throw new Exception(String.format("Try to connect to unstable config store ", cs.getStoreURI()));
      }
    }
    
    // ConfigStoreFactory scheme name could be different than configStore's StoreURI's scheme name
    URI csRoot = cs.getStoreURI();
    URI key = new URI(csFactory.getScheme(), csRoot.getAuthority(), csRoot.getPath(), csRoot.getQuery(), csRoot.getFragment());
    ConfigStoreAccessor value = new ConfigStoreAccessor(cs, cs.getCurrentVersion());
    this.configStoreMap.put(key, value);
    return value;
  }

  // TBD MITU, need to use serviceLoader
  // NEED to cache the mapping?
  private ConfigStoreFactory<ConfigStore> getConfigStoreFactory(URI uri) throws Exception {
    return new ETLHdfsConfigStoreFactory();
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
   *    One of the implementation for ConfigStore to determine the store root is
   *    by back tracing the input uri, the path which contains "_CONFIG_STORE" is the root
   * 4. Build ConfigStoreAccessor by checking the current version of the ConfigStore. Added the entry to theMap
   * 
   * 5. If the ConfigStore is NOT ConfigStoreWithResolution, need to do resolution in this client
   */
  public Config getConfig(URI uri) throws Exception{
    
    ConfigStoreAccessor csa = this.getConfigStoreAccessor(uri);
    
    URI rel_uri = getRelativeUriToStore(csa.store.getStoreURI(), uri);
    
    if(csa.store instanceof ConfigStoreWithResolution){
      return ((ConfigStoreWithResolution) csa.store).getResolvedConfig(rel_uri, csa.version);
    }
    
    SimpleConfigStoreResolver resolver = new SimpleConfigStoreResolver(csa.store);
    return resolver.getResolvedConfig(rel_uri, csa.version);
  }

  /**
   * @param uris - Collection of URI, each one much start with scheme name
   * @return - the java.util.Map. Key of the map is the URI, value of the Map is getConfig(URI key)
   */
  public Map<URI, Config> getConfigs(Collection<URI> uris) throws Exception{
    Map<URI, Config> result = new HashMap<URI, Config>();
    Iterator<URI> it = uris.iterator();
    
    URI tmp;
    while(it.hasNext()){
      tmp = it.next();
      result.put(tmp, this.getConfig(tmp));
    }
    return result;
  }

  /**
   * 
   * @param uri - URI which must start with scheme name
   * @param recursive - indicate to get the imported URI recursively or not
   * @return The java.util.Collection which contains all the URI imported by input uri
   * All the URIs must starts with scheme names
   * @throws Exception 
   */
  public Collection<URI> getImports(URI uri, boolean recursive) throws Exception {
    ConfigStoreAccessor csa = this.getConfigStoreAccessor(uri);
    
    URI rel_uri = getRelativeUriToStore(csa.store.getStoreURI(), uri);
    
    if(!recursive){
      return getAbsoluteUri(uri.getScheme(), csa.store.getStoreURI(), csa.store.getOwnImports(rel_uri, csa.version));
    }
    
    // need to get recursively imports 
    if(csa.store instanceof ConfigStoreWithResolution){
      return getAbsoluteUri(uri.getScheme(), csa.store.getStoreURI(),
          ((ConfigStoreWithResolution) csa.store).getImportsRecursively(rel_uri, csa.version));
    }
    
    SimpleImportMappings im = new SimpleImportMappings(csa.store, csa.version);
    return getAbsoluteUri(uri.getScheme(), csa.store.getStoreURI(),
        im.getImportMappingRecursively().get(rel_uri));
  }

  /**
   * 
   * @param uri - URI which must start with scheme name
   * @param recursive - indicate to get the imported by URI recursively or not
   * @return The java.util.Collection which contains all the URI which import input uri
   * @throws Exception 
   */
  public Collection<URI> getImportedBy(URI uri, boolean recursive) throws Exception {
    ConfigStoreAccessor csa = this.getConfigStoreAccessor(uri);
    URI rel_uri = getRelativeUriToStore(csa.store.getStoreURI(), uri);
    
    if((!recursive) && (csa.store instanceof ConfigStoreWithImportedBy)){
      return getAbsoluteUri(uri.getScheme(), csa.store.getStoreURI(),
          ((ConfigStoreWithImportedBy) csa.store).getImportedBy(rel_uri,csa.version));
    }
    
    if(recursive && (csa.store instanceof ConfigStoreWithImportedByRecursively)){
      return getAbsoluteUri(uri.getScheme(), csa.store.getStoreURI(),
          ((ConfigStoreWithImportedByRecursively) csa.store).getImportedByRecursively(rel_uri, csa.version));
    }
    
    SimpleImportMappings im = new SimpleImportMappings(csa.store, csa.version);
    if(!recursive){
      return getAbsoluteUri(uri.getScheme(), csa.store.getStoreURI(), im.getImportedByMapping().get(rel_uri));
    }
    
    return getAbsoluteUri(uri.getScheme(), csa.store.getStoreURI(), im.getImportMappingRecursively().get(rel_uri));

  }

  /**
   * 
   * @param uri - clean the cache for the configuration store which specified by input URI 
   * This will cause a new version of the URI to be retrieved next time it is called, which could be 
   * different from the last configuration received.
   */
  public void clearCache(URI uri) {
    this.configStoreMap.remove(uri);
  }

  private URI getRelativeUriToStore(URI storeRootURI, URI absURI) throws URISyntaxException{
    String root = storeRootURI.getPath();
    String absPath = absURI.getPath();
    
    if(root.equals(absPath)){
      return new URI("");
    }
    
    return new URI(absPath.substring(root.length()+1));
  }
  
  private Collection<URI> getAbsoluteUri(String scheme, URI storeRootURI, Collection<URI> relativeURI) throws URISyntaxException {
    List<URI> result = new ArrayList<URI>();
    
    Iterator<URI> it = relativeURI.iterator();
    URI tmp;
    while(it.hasNext()){
      tmp = it.next();
      result.add( new URI(scheme, 
          storeRootURI.getAuthority(),
          storeRootURI.getPath() + "/" + tmp.getPath(),
          storeRootURI.getQuery(),
          storeRootURI.getFragment()) );
    }
    return result;
  }
  
  static class ConfigStoreAccessor {
    String version;
    ConfigStore store;
    ConfigStoreAccessor(ConfigStore cs, String v){
      this.store = cs;
      this.version = v;
    }
  }
}
