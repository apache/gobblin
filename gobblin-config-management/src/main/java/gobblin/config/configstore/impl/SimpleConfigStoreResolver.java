package gobblin.config.configstore.impl;

import gobblin.config.configstore.ConfigStore;
import gobblin.config.configstore.ConfigStoreWithResolution;
import gobblin.config.utils.CircularDependencyChecker;
import gobblin.config.utils.PathUtils;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import com.typesafe.config.Config;


public class SimpleConfigStoreResolver implements ConfigStoreWithResolution {

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
  public Collection<URI> getChildren(URI uri, String version) {
    return store.getChildren(uri, version);
  }

  @Override
  public Collection<URI> getOwnImports(URI uri, String version) {
    return store.getOwnImports(uri, version);
  }

  @Override
  public Config getOwnConfig(URI uri, String version) {
    return store.getOwnConfig(uri, version);
  }

  @Override
  public Config getResolvedConfig(URI uri, String version) {
    CircularDependencyChecker.checkCircularDependency(store, version, uri);

    Config self = this.getOwnConfig(uri, version);

    // root can not include anything, otherwise will have circular dependency 
    if (isRootURI(uri)) {
      return self;
    }

    Collection<URI> imported = this.getOwnImports(uri, version);
    List<Config> importedConfigs = new ArrayList<Config>();
    for (URI u : imported) {
      importedConfigs.add(this.getResolvedConfig(u, version));
    }

    // apply the reverse order for imported
    for (int i = importedConfigs.size() - 1; i >= 0; i--) {
      self = self.withFallback(importedConfigs.get(i));
    }

    Config ancestor = this.getAncestorConfig(uri, version);
    return self.withFallback(ancestor);
  }

  protected Config getAncestorConfig(URI uri, String version) {
    URI parent = PathUtils.getParentURI(uri);
    Config res = getResolvedConfig(parent, version);
    return res;
  }

  protected static final boolean isRootURI(URI uri) {
    if (uri == null)
      return false;

    return uri.toString().length() == 0;
  }

  @Override
  public Collection<URI> getImportsRecursively(URI uri, String version) {
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
