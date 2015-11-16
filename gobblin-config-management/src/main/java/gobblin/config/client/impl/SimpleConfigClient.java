package gobblin.config.client.impl;

import java.net.URI;
import java.util.Collection;

import com.typesafe.config.Config;

import gobblin.config.client.ConfigClient;
import gobblin.config.configstore.ConfigStore;
import gobblin.config.configstore.ConfigStoreWithResolution;

public class SimpleConfigClient implements ConfigClient{

  @Override
  public Config getConfig(ConfigStore cs, URI uri) {
    if(cs instanceof ConfigStoreWithResolution) {
      return ((ConfigStoreWithResolution)cs).getResolvedConfig(uri);
    }
    
    return cs.getOwnConfig(uri);
  }

  @Override
  public Collection<URI> getImported(ConfigStore cs, URI uri) {
    if(cs instanceof ConfigStoreWithResolution) {
      return ((ConfigStoreWithResolution)cs).getResolvedImports(uri);
    }
    
    return cs.getOwnImports(uri);
  }

}
