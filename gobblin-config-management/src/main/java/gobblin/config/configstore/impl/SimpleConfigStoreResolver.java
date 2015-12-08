package gobblin.config.configstore.impl;

import gobblin.config.configstore.ConfigStore;
import gobblin.config.configstore.ConfigStoreWithResolution;
import gobblin.config.configstore.VersionDoesNotExistException;
import gobblin.config.utils.CircularDependencyChecker;
import gobblin.config.utils.PathUtils;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;


public class SimpleConfigStoreResolver implements ConfigStoreWithResolution {

  private static final Logger LOG = Logger.getLogger(SimpleConfigStoreResolver.class);
  private final ConfigStore store;

  public SimpleConfigStoreResolver(ConfigStore base) {
    this.store = base;
  }

  @Override
  public String getCurrentVersion() {
    return store.getCurrentVersion();
  }

  @Override
  public URI getStoreURI() {
    return store.getStoreURI();
  }

  @Override
  public Collection<URI> getChildren(URI uri, String version) throws VersionDoesNotExistException{
    return store.getChildren(uri, version);
  }

  @Override
  public Collection<URI> getOwnImports(URI uri, String version) throws VersionDoesNotExistException{
    return store.getOwnImports(uri, version);
  }

  @Override
  public Config getOwnConfig(URI uri, String version) throws VersionDoesNotExistException{
    return store.getOwnConfig(uri, version);
  }

  @Override
  public Config getResolvedConfig(URI uri, String version) throws VersionDoesNotExistException{
    UUID traceID = UUID.randomUUID();
    LOG.info(String.format("Getting resolved config for uri: %s for version %s, with traceID: %s", 
        uri, version, traceID));
    return getResolvedConfigWithTrace(uri, version, traceID.toString());
  }

  protected Config getResolvedConfigWithTrace(URI uri, String version, String traceID) throws VersionDoesNotExistException{
    CircularDependencyChecker.checkCircularDependency(store, version, uri);
    Config self = this.getOwnConfig(uri, version);
    for (Entry<String, ConfigValue> entry : self.entrySet()) {
      LOG.info(String.format("key %s, value %s from uri %s, with traceID %s", 
          entry.getKey(), entry.getValue(), uri, traceID)); 
    }

    // root can not include anything, otherwise will have circular dependency 
    if (isRootURI(uri)) {
      return self;
    }

    Collection<URI> imported = this.getOwnImports(uri, version);
    List<Config> importedConfigs = new ArrayList<Config>();
    for (URI u : imported) {
      Config singleImported = this.getResolvedConfigWithTrace(u, version, traceID);
      importedConfigs.add(singleImported);
    }

    // apply the reverse order for imported
    for (int i = importedConfigs.size() - 1; i >= 0; i--) {
      self = self.withFallback(importedConfigs.get(i));
    }

    Config ancestor = this.getAncestorConfig(uri, version, traceID);
    return self.withFallback(ancestor);
  }

  protected Config getAncestorConfig(URI uri, String version, String traceID) throws VersionDoesNotExistException{
    URI parent = PathUtils.getParentURI(uri);
    Config res = getResolvedConfigWithTrace(parent, version, traceID);
    return res;
  }

  protected static final boolean isRootURI(URI uri) {
    if (uri == null)
      return false;

    return uri.toString().length() == 0;
  }

  @Override
  public Collection<URI> getImportsRecursively(URI uri, String version) throws VersionDoesNotExistException{
    CircularDependencyChecker.checkCircularDependency(this, version, uri);

    Collection<URI> result = getOwnImports(uri, version);
    // root can not include anything, otherwise will have circular dependency 
    if (isRootURI(uri)) {
      return result;
    }

    Collection<URI> imported = this.getOwnImports(uri, version);
    for (URI u : imported) {
      result.addAll(this.getImportsRecursively(u, version));
    }

    result.addAll(this.getImportsRecursively(PathUtils.getParentURI(uri), version));

    return dedup(result);
  }

  protected Collection<URI> dedup(Collection<URI> input) {
    Set<URI> set = new LinkedHashSet<URI>(input);
    return set;
  }
}
