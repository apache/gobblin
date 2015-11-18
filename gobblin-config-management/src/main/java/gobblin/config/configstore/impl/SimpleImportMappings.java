package gobblin.config.configstore.impl;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.log4j.Logger;

import gobblin.config.configstore.ConfigStore;
import gobblin.config.configstore.ConfigStoreWithResolution;
import gobblin.config.configstore.ImportMappings;


public class SimpleImportMappings implements ImportMappings {

  private static final Logger LOG = Logger.getLogger(SimpleImportMappings.class);

  private final Map<URI, Collection<URI>> importMap = new HashMap<URI, Collection<URI>>();
  private final Map<URI, Collection<URI>> importedByMap = new HashMap<URI, Collection<URI>>();
  private final Map<URI, Collection<URI>> importMapResursively = new HashMap<URI, Collection<URI>>();
  private final Map<URI, Collection<URI>> importedByMapRecursively = new HashMap<URI, Collection<URI>>();

  private final ConfigStore store;
  private final String version;


  public SimpleImportMappings(ConfigStore cs, String version) {
    this.store = cs;
    this.version = version;
    buildMaps();
  }

  private void buildMaps() {
    try {
      URI root = new URI("");
      Collection<URI> currentLevel = this.store.getChildren(root, version);

      while (currentLevel.size() != 0) {
        Collection<URI> nextLevel = new ArrayList<URI>();
        for (URI u : currentLevel) {
          Collection<URI> ownImported = this.store.getOwnImports(u, version);
          this.addToImportMapping(u, ownImported, this.importMap, this.importedByMap);
          
          if(this.store instanceof ConfigStoreWithResolution){
            Collection<URI> resolved = ((ConfigStoreWithResolution) this.store).getImportsRecursively(u, version);
            this.addToImportMapping(u, resolved, this.importMapResursively, this.importedByMapRecursively);
          }
          
          nextLevel.addAll(this.store.getChildren(u, version));
        }
        currentLevel = nextLevel;
      }
    } catch (URISyntaxException e) {
      LOG.error("Got Exception: " + e.getMessage(), e);
      throw new RuntimeException(e);
    }
  }
  
  private void addToImportMapping(URI u, Collection<URI> imported, 
      Map<URI, Collection<URI>> targetImportMap, Map<URI, Collection<URI>> targetImportedByMap){
    if (!targetImportMap.containsKey(u)) {
      targetImportMap.put(u, new HashSet<URI>());
    }
    targetImportMap.get(u).addAll(imported);
    this.addToImportedByMapping(u, imported, targetImportedByMap);
  }

  private void addToImportedByMapping(URI u, Collection<URI> imported, Map<URI, Collection<URI>> targetImportedByMap) {
    for (URI one_imported : imported) {
      if (!targetImportedByMap.containsKey(one_imported)) {
        targetImportedByMap.put(one_imported, new HashSet<URI>());
      }

      targetImportedByMap.get(one_imported).add(u);
    }
  }


  @Override
  public Map<URI, Collection<URI>> getImportMapping() {
    return this.importMap;
  }

  @Override
  public Map<URI, Collection<URI>> getImportedByMapping() {
    return this.importedByMap;
  }

  @Override
  public Map<URI, Collection<URI>> getImportMappingRecursively() {
    return this.importMapResursively;
  }

  @Override
  public Map<URI, Collection<URI>> getImportedByMappingRecursively() {
    return this.importedByMapRecursively;
  }
}
