package gobblin.config.configstore;

import java.net.URI;
import java.util.Collection;

import com.typesafe.config.Config;

public interface ConfigStoreWithResolution extends ConfigStore{
  
  public Config getResolvedConfig(URI uri);
  
  public Collection<URI> getResolvedImports(URI uri);

}
