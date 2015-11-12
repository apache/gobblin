package gobblin.config.utils;

import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import gobblin.config.configstore.ConfigStore;
import gobblin.config.configstore.ConfigStoreWithResolution;

public class ImportMappingBuilder {

  private static Collection<URI> getImportedFromConfigStore(ConfigStore cs, URI uri){
    if(cs instanceof ConfigStoreWithResolution){
      return ((ConfigStoreWithResolution)cs).getResolvedImports(uri);
    }
    
    return cs.getOwnImports(uri);
  }
  public static Map<URI, Collection<URI>> buildImportMapping(ConfigStore cs){
    Map<URI, Collection<URI>> result = new HashMap<URI, Collection<URI>>();
    
    
    return result;
  }
  
  public static Map<URI, Collection<URI>> buildImportedByMapping(ConfigStore cs){
    Map<URI, Collection<URI>> result = new HashMap<URI, Collection<URI>>();
    
    return result;
  }
}
