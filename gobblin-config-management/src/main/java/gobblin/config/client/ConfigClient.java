package gobblin.config.client;

import gobblin.config.configstore.ConfigStore;
import gobblin.config.configstore.ConfigStoreCreationException;
import gobblin.config.configstore.ConfigStoreFactory;
import gobblin.config.configstore.ConfigStoreFactoryDoesNotExistsException;
import gobblin.config.configstore.ConfigStoreWithImportedBy;
import gobblin.config.configstore.ConfigStoreWithImportedByRecursively;
import gobblin.config.configstore.ConfigStoreWithResolution;
import gobblin.config.configstore.ConfigStoreWithStableVersion;
import gobblin.config.configstore.VersionDoesNotExistException;
import gobblin.config.configstore.impl.SimpleConfigStoreResolver;
import gobblin.config.configstore.impl.SimpleImportMappings;
import gobblin.config.utils.PathUtils;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.typesafe.config.Config;


/**
 * ConfigClient is the consumer facing class for retrieving configuration against all configuration stores.
 * @author mitu
 *
 */
@SuppressWarnings("rawtypes")
public class ConfigClient {

  private static final Logger LOG = Logger.getLogger(ConfigClient.class);

  public static enum VersionStabilityPolicy {
    RAISE_ERROR,
    CACHE_CONFIG_IN_MEMORY
  }

  private final VersionStabilityPolicy policy;

  // key is the store ROOT, must use TreeMap
  private final TreeMap<URI, ConfigStoreAccessor> configStoreMap = new TreeMap<URI, ConfigStoreAccessor>();

  // key is the configStore scheme name, value is the ConfigStoreFactory
  private final Map<String, ConfigStoreFactory> configStoreFactoryMap = new HashMap<String, ConfigStoreFactory>();

  private ConfigClient(VersionStabilityPolicy policy) {
    this.policy = policy;

    ServiceLoader<ConfigStoreFactory> loader = ServiceLoader.load(ConfigStoreFactory.class);
    for (ConfigStoreFactory f : loader) {
      configStoreFactoryMap.put(f.getScheme(), f);
      LOG.info("Created the config store factory with scheme name " + f.getScheme());
    }
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
  public static ConfigClient createConfigClientWithStableVersion(VersionStabilityPolicy policy) {
    return new ConfigClient(policy);
  }

  /**
   * 
   * @return the {@ConfigClient} with policy set to VERSION_STABILITY_POLICY.RAISE_ERROR
   */
  public static ConfigClient createDefaultConfigClient() {
    // create with stable versions
    return createConfigClientWithStableVersion(VersionStabilityPolicy.RAISE_ERROR);
  }

  private ConfigStoreAccessor getConfigStoreAccessor(URI uri) throws ConfigStoreFactoryDoesNotExistsException,
      ConfigStoreCreationException, VersionDoesNotExistException {
    URI floorKey = this.configStoreMap.floorKey(uri);
    if (PathUtils.checkDescendant(floorKey, uri)) {
      return this.configStoreMap.get(floorKey);
    }

    ConfigStoreFactory<ConfigStore> csFactory = this.getConfigStoreFactory(uri);
    ConfigStore cs = csFactory.createConfigStore(uri);

    if (!(cs instanceof ConfigStoreWithStableVersion)) {
      if (this.policy == VersionStabilityPolicy.RAISE_ERROR) {
        throw new RuntimeException(String.format("Try to connect to unstable config store ", cs.getStoreURI()));
      }
    }

    URI csRootURI = cs.getStoreURI();
    ConfigStoreAccessor CSAccessor = new ConfigStoreAccessor(cs, cs.getCurrentVersion());

    // create default resolver
    if (!(cs instanceof ConfigStoreWithResolution)) {
      SimpleConfigStoreResolver resolver = new SimpleConfigStoreResolver(cs);
      CSAccessor.resolver = resolver;
    }

    // create default importedBy mapping object
    if (!((cs instanceof ConfigStoreWithImportedBy) && (cs instanceof ConfigStoreWithImportedByRecursively))) {
      SimpleImportMappings im = null;
      // need to create the SimpleImportMappings using the resolver NOT the raw configStore
      // otherwise, the getImportsRecursively and getImportedByRecursively will not work
      // as raw configstore is NOT ConfigStoreWithResolution
      if (!(cs instanceof ConfigStoreWithResolution)) {
        im = new SimpleImportMappings(CSAccessor.resolver, CSAccessor.version);
      } else {
        im = new SimpleImportMappings(cs, CSAccessor.version);
      }
      CSAccessor.simpleImportMappings = im;
    }

    this.configStoreMap.put(csRootURI, CSAccessor);
    LOG.info(String.format("Created new config store with uri: %s, version: %s", cs.getStoreURI(),
        cs.getCurrentVersion()));
    return CSAccessor;
  }

  // use serviceLoader to load configStoreFactories
  @SuppressWarnings("unchecked")
  private ConfigStoreFactory<ConfigStore> getConfigStoreFactory(URI uri)
      throws ConfigStoreFactoryDoesNotExistsException {
    ConfigStoreFactory csf = configStoreFactoryMap.get(uri.getScheme());
    if (csf == null) {
      throw new ConfigStoreFactoryDoesNotExistsException("can not find corresponding config store factory for scheme "
          + uri.getScheme());
    }

    return (ConfigStoreFactory<ConfigStore>) csf;
  }

  /**
   * 
   * @param configKeyUri - must start with scheme name
   * @return - the directly and indirectly specified configuration in {@com.typesafe.config.Config} format for input uri 
   * 
   * <p>
   * detail logics:
   * 1. Used the ConfigStoreAccessor in the cached map if previous ConfigStore been queried
   * 2. Based the scheme name, using {@java.util.ServiceLoader} to find the first {@gobblin.config.configstore.ConfigStoreFactory}
   * 3. Use ConfigStoreFactory to create ConfigStore 
   * 4. Build ConfigStoreAccessor by checking the current version of the ConfigStore. Added the entry to theMap
   * 5. If the ConfigStore is NOT ConfigStoreWithResolution, need to do resolution in this client
   */
  public Config getConfig(URI configKeyUri) throws ConfigStoreFactoryDoesNotExistsException, ConfigStoreCreationException,
      VersionDoesNotExistException {

    ConfigStoreAccessor csa = this.getConfigStoreAccessor(configKeyUri);
    URI rel_uri = csa.store.getStoreURI().relativize(configKeyUri);

    if (csa.store instanceof ConfigStoreWithResolution) {
      return ((ConfigStoreWithResolution) csa.store).getResolvedConfig(rel_uri, csa.version);
    }

    SimpleConfigStoreResolver resolver = csa.resolver;
    return resolver.getResolvedConfig(rel_uri, csa.version);
  }
  
  public Config getConfig(String configKey) throws ConfigStoreFactoryDoesNotExistsException, ConfigStoreCreationException,
    VersionDoesNotExistException, URISyntaxException {
    return this.getConfig(new URI(configKey));
  }

  /**
   * @param configKeyUris - Collection of URI, each one must start with scheme name
   * @return - the java.util.Map. Key of the map is the URI, value of the Map is getConfig(URI key)
   */
  // TODO, if the number configKeyUris is large, we can parallelize the calls using the ThreadPool.
  public Map<URI, Config> getConfigs(Collection<URI> configKeyUris) throws ConfigStoreFactoryDoesNotExistsException,
      ConfigStoreCreationException, VersionDoesNotExistException {
    Map<URI, Config> result = new HashMap<URI, Config>();
    for (URI tmp : configKeyUris) {
      result.put(tmp, this.getConfig(tmp));
    }
    return result;
  }

  public Map<URI, Config> getConfigsFromStrings(Collection<String> configKeys) throws ConfigStoreFactoryDoesNotExistsException,
      ConfigStoreCreationException, VersionDoesNotExistException, URISyntaxException {
    if(configKeys==null || configKeys.size()==0){
      return Collections.emptyMap();
    }
    
    Collection<URI> configKeyUris = new ArrayList<URI>();
    for(String s: configKeys){
      configKeyUris.add(new URI(s));
    }
    
    return getConfigs(configKeyUris);
  }

  /**
   * 
   * @param configKeyUri - URI which must start with scheme name
   * @param recursive - indicate to get the imported URI recursively or not
   * @return The java.util.Collection which contains all the URI imported by input uri
   * All the URIs must starts with scheme names
   * @throws ConfigStoreFactoryDoesNotExistsException - if can not find the configuration store factory with the schema name
   *  provided in the uri
   * @throws ConfigStoreCreationException - if can not create the configuration store based on the uri
   * @throws VersionDoesNotExistException - as the version is cached in the {@ConfigStoreAccessor}, if the version
   *  not exist on the config store anymore, this Exception will be thrown
   */
  public Collection<URI> getImports(URI configKeyUri, boolean recursive) throws ConfigStoreFactoryDoesNotExistsException,
      ConfigStoreCreationException, VersionDoesNotExistException {
    ConfigStoreAccessor csa = this.getConfigStoreAccessor(configKeyUri);
    URI rel_uri = csa.store.getStoreURI().relativize(configKeyUri);

    if (!recursive) {
      return getAbsoluteUri(csa.store.getStoreURI(), csa.store.getOwnImports(rel_uri, csa.version));
    }

    // need to get recursively imports 
    if (csa.store instanceof ConfigStoreWithResolution) {
      return getAbsoluteUri(csa.store.getStoreURI(),
          ((ConfigStoreWithResolution) csa.store).getImportsRecursively(rel_uri, csa.version));
    }

    SimpleImportMappings im = csa.simpleImportMappings;
    return getAbsoluteUri(csa.store.getStoreURI(), im.getImportMappingRecursively().get(rel_uri));
  }

  /**
   * 
   * @param configKeyUri - URI which must start with scheme name
   * @param recursive - indicate to get the imported by URI recursively or not
   * @return The java.util.Collection which contains all the URI which import input uri
   * 
   * @throws ConfigStoreFactoryDoesNotExistsException - if can not find the configuration store factory with the schema name
   *  provided in the uri
   * @throws ConfigStoreCreationException - if can not create the configuration store based on the uri
   * @throws VersionDoesNotExistException - as the version is cached in the {@ConfigStoreAccessor}, if the version
   *  not exist on the config store anymore, this Exception will be thrown
   */
  public Collection<URI> getImportedBy(URI configKeyUri, boolean recursive) throws ConfigStoreFactoryDoesNotExistsException,
      ConfigStoreCreationException, VersionDoesNotExistException {
    ConfigStoreAccessor csa = this.getConfigStoreAccessor(configKeyUri);
    URI rel_uri = csa.store.getStoreURI().relativize(configKeyUri);

    if ((!recursive) && (csa.store instanceof ConfigStoreWithImportedBy)) {
      return getAbsoluteUri(csa.store.getStoreURI(),
          ((ConfigStoreWithImportedBy) csa.store).getImportedBy(rel_uri, csa.version));
    }

    if (recursive && (csa.store instanceof ConfigStoreWithImportedByRecursively)) {
      return getAbsoluteUri(csa.store.getStoreURI(),
          ((ConfigStoreWithImportedByRecursively) csa.store).getImportedByRecursively(rel_uri, csa.version));
    }

    SimpleImportMappings im = csa.simpleImportMappings;

    if (!recursive) {
      return getAbsoluteUri(csa.store.getStoreURI(), im.getImportedByMapping().get(rel_uri));
    }

    return getAbsoluteUri(csa.store.getStoreURI(), im.getImportedByMappingRecursively().get(rel_uri));
  }

  /**
   * 
   * @param configKeyUri - clean the cache for the configuration store which specified by input URI 
   * This will cause a new version of the URI to be retrieved next time it is called, which could be 
   * different from the last configuration received.
   */
  public void clearCache(URI configKeyUri) {
    URI floorKey = this.configStoreMap.floorKey(configKeyUri);
    if (PathUtils.checkDescendant(floorKey, configKeyUri)) {
      ConfigStoreAccessor csa = this.configStoreMap.remove(floorKey);
      LOG.info(String.format("Cleared cache for config store: %s, version %s", csa.store.getStoreURI(), csa.version));
    }
  }

  private Collection<URI> getAbsoluteUri(URI storeRootURI, Collection<URI> relativeURI) {
    List<URI> result = new ArrayList<URI>();
    if (relativeURI == null || relativeURI.size() == 0)
      return result;

    for (URI tmp : relativeURI) {
      try {
        result.add(new URI(storeRootURI.getScheme(), storeRootURI.getAuthority(), storeRootURI.getPath() + "/"
            + tmp.getPath(), storeRootURI.getQuery(), storeRootURI.getFragment()));
      } catch (URISyntaxException e) {
        // should NOT come here
        LOG.error(String.format("Got error when process root uri %s with relative uri %s", storeRootURI, tmp));
      }
    }
    return result;
  }

  static class ConfigStoreAccessor {
    String version;
    ConfigStore store;
    SimpleImportMappings simpleImportMappings;
    SimpleConfigStoreResolver resolver;

    ConfigStoreAccessor(ConfigStore cs, String v) {
      this.store = cs;
      this.version = v;
    }
  }
}
